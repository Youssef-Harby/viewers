import { useState, useEffect, useRef } from 'react';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { load } from '@loaders.gl/core';
import { ParquetLoader } from '@loaders.gl/parquet';
import { wkbToGeojson } from '@loaders.gl/gis';
import { ZstdCodec } from 'zstd-codec';
import Map from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';

// Constants
const CHUNK_SIZE = 1000000; // Same chunk size as loaders.gl
const INITIAL_VIEW_STATE = {
  latitude: 0,
  longitude: 0,
  zoom: 1,
  bearing: 0,
  pitch: 0
};

// Keep ZSTD instance at module level
let zstdPromise: Promise<any>;
let zstd: any;

interface ParquetLayerProps {
  url: string;
  onLoad?: () => void;
  onError?: (error: Error) => void;
}

const ParquetLayer: React.FC<ParquetLayerProps> = ({ url, onLoad, onError }) => {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<Error | null>(null);
  const [viewState, setViewState] = useState(INITIAL_VIEW_STATE);
  const deckRef = useRef<any>(null);

  useEffect(() => {
    let isMounted = true;

    const preloadZstd = async () => {
      if (!zstdPromise) {
        zstdPromise = new Promise((resolve) => ZstdCodec.run((codec) => resolve(codec)));
        zstd = await zstdPromise;
      }
      return zstd;
    };

    const decompressData = async (input: ArrayBuffer): Promise<ArrayBuffer> => {
      const codec = await preloadZstd();
      const simpleZstd = new codec.Streaming();
      const inputArray = new Uint8Array(input);

      const chunks: Uint8Array[] = [];
      for (let i = 0; i <= inputArray.length; i += CHUNK_SIZE) {
        chunks.push(inputArray.subarray(i, i + CHUNK_SIZE));
      }

      const decompressResult = await simpleZstd.decompressChunks(chunks);
      return decompressResult.buffer;
    };

    const createPointFromCoordinates = (lat: number, lon: number) => {
      return {
        type: 'Point',
        coordinates: [parseFloat(lon), parseFloat(lat)]
      };
    };

    const loadData = async () => {
      try {
        console.log('Loading Parquet data from:', url);

        // Fetch the Parquet file as an ArrayBuffer
        const response = await fetch(url);
        const arrayBuffer = await response.arrayBuffer();
        console.log('Fetched data size:', arrayBuffer.byteLength);

        // Try loading directly first
        try {
          const parquetData = await load(arrayBuffer, ParquetLoader, {
            parquet: {
              shape: 'object-row-table'
            }
          });
          
          console.log('Successfully loaded without decompression');
          await processParquetData(parquetData);
          return;
        } catch (err) {
          console.log('Direct loading failed, trying with ZSTD:', err);
        }

        // Try with ZSTD decompression
        console.log('Attempting ZSTD decompression...');
        const decompressedData = await decompressData(arrayBuffer);
        if (!decompressedData) {
          throw new Error('ZSTD decompression failed - no data returned');
        }

        console.log('Decompressed size:', decompressedData.byteLength);

        // Load the decompressed data as Parquet
        const parquetData = await load(decompressedData, ParquetLoader, {
          parquet: {
            shape: 'object-row-table'
          }
        });

        await processParquetData(parquetData);
      } catch (err) {
        console.error('Error loading Parquet data:', err);
        setError(err as Error);
        if (onError) onError(err as Error);
      }
    };

    const processParquetData = async (parquetData: any) => {
      if (!isMounted) return;

      console.log('Processing Parquet data...');
      
      let features = [];
      const geometryColumns = ['geometry', 'geom', 'the_geom', 'wkb_geometry'];
      let geometryColumn = null;

      // Find geometry column from schema
      if (parquetData.schema) {
        for (const colName of geometryColumns) {
          if (parquetData.schema.fields.some(f => f.name === colName)) {
            geometryColumn = colName;
            break;
          }
        }
        console.log('Found geometry column:', geometryColumn);
        console.log('Available columns:', parquetData.schema?.fields.map(f => f.name));
      }

      if (!geometryColumn) {
        // Handle lat/lon case
        if (parquetData.schema?.fields.some(f => f.name === 'latitude') && 
            parquetData.schema?.fields.some(f => f.name === 'longitude')) {
          console.log('Using latitude/longitude for geometry');
          const rows = Array.isArray(parquetData) ? parquetData : parquetData.data;
          for (const row of rows) {
            if (row.latitude && row.longitude) {
              features.push({
                type: 'Feature',
                geometry: createPointFromCoordinates(row.latitude, row.longitude),
                properties: row
              });
            }
          }
        } else {
          throw new Error('No geometry column or lat/lon columns found in Parquet data');
        }
      } else {
        // Process rows with geometry column
        const rows = Array.isArray(parquetData) ? parquetData : parquetData.data;
        console.log('Processing', rows.length, 'rows');

        for (const row of rows) {
          let geometry = null;
          const rawGeometry = row[geometryColumn];
          
          if (!rawGeometry) {
            continue;
          }

          try {
            // Case 1: Handle WKB format (most likely case)
            if (rawGeometry instanceof Uint8Array || ArrayBuffer.isView(rawGeometry)) {
              try {
                geometry = wkbToGeojson(rawGeometry);
              } catch (err) {
                console.debug('WKB parsing failed:', err);
              }
            }
            
            // Case 2: Handle string format (less likely)
            if (!geometry && typeof rawGeometry === 'string') {
              // Skip JSON parsing attempt if it looks like WKB hex
              if (!/^[0-9a-fA-F]+$/.test(rawGeometry)) {
                try {
                  const parsed = JSON.parse(rawGeometry);
                  if (parsed && parsed.type && parsed.coordinates) {
                    geometry = parsed;
                  }
                } catch (err) {
                  // Silently ignore JSON parsing errors
                }
              }
            }
            
            // Case 3: Already a GeoJSON object
            if (!geometry && rawGeometry.type && rawGeometry.coordinates) {
              geometry = rawGeometry;
            }

            // Fallback to lat/lon
            if (!geometry && row.latitude && row.longitude) {
              geometry = createPointFromCoordinates(row.latitude, row.longitude);
            }

            if (geometry) {
              features.push({
                type: 'Feature',
                geometry,
                properties: {...row, [geometryColumn]: undefined}
              });
            }
          } catch (err) {
            console.debug('Failed to process row:', err);
          }
        }
      }

      if (features.length === 0) {
        throw new Error('No valid features found in Parquet data');
      }

      console.log('Successfully processed', features.length, 'features');

      const geojson = {
        type: 'FeatureCollection',
        features
      };

      if (isMounted) {
        setData(geojson);
        if (onLoad) onLoad();
      }
    };

    loadData();

    return () => {
      isMounted = false;
    };
  }, [url, onLoad, onError]);

  if (error) {
    return <div>Error loading Parquet data: {error.message}</div>;
  }

  if (!data) {
    return <div>Loading...</div>;
  }

  return (
    <DeckGL
      ref={deckRef}
      initialViewState={INITIAL_VIEW_STATE}
      controller={true}
      layers={[
        new GeoJsonLayer({
          id: 'geojson',
          data: data,
          filled: true,
          pointRadiusMinPixels: 2,
          pointRadiusScale: 2000,
          getPointRadius: 1,
          getFillColor: [255, 0, 0, 128],
          pickable: true,
          autoHighlight: true
        })
      ]}
    >
      <Map
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        onMove={evt => setViewState(evt.viewState)}
      />
    </DeckGL>
  );
};

export default ParquetLayer;
