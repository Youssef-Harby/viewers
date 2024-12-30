import React, { useState, useEffect, useRef } from 'react';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { load, registerLoaders } from '@loaders.gl/core';
import { ParquetLoader } from '@loaders.gl/parquet';
import * as wkx from 'wkx';
import Map, { ViewState } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { INITIAL_VIEW_STATE } from './constants';

registerLoaders([ParquetLoader]);

interface ParquetLayerProps {
  url: string;
  onLoad?: (data: any) => void;
  onError?: (error: Error) => void;
  layerProps?: Record<string, any>;
}

const ParquetLayer: React.FC<ParquetLayerProps> = ({ url, onLoad, onError, layerProps = {} }) => {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<Error | null>(null);
  const [viewState, setViewState] = useState<Record<string, any>>(INITIAL_VIEW_STATE);
  const deckRef = useRef<any>(null);

  useEffect(() => {
    let isMounted = true;

    const loadParquetData = async () => {
      try {
        console.log('Loading Parquet data from:', url);

        const options = {
          parquet: {
            shape: 'object-row-table',
            preserveBinary: true
          }
        };

        const result = await load(url, ParquetLoader, options);
        
        if (!result || !Array.isArray(result)) {
          throw new Error('No data returned from ParquetLoader');
        }

        console.log('Loaded Parquet data:', result);

        if (isMounted) {
          const features = processParquetData(result);
          
          // Calculate bounds for initial view
          if (features.features.length > 0) {
            const bounds = getBounds(features.features);
            if (bounds) {
              setViewState({
                ...viewState,
                longitude: (bounds[0] + bounds[2]) / 2,
                latitude: (bounds[1] + bounds[3]) / 2,
                zoom: 8
              });
            }
          }

          setData(features);
          if (onLoad) onLoad(features);
        }
      } catch (err) {
        console.error('Error loading Parquet data:', err);
        if (isMounted) {
          setError(err as Error);
          if (onError) onError(err as Error);
        }
      }
    };

    loadParquetData();

    return () => {
      isMounted = false;
    };
  }, [url]);

  const getBounds = (features: any[]): number[] | null => {
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;

    features.forEach(feature => {
      const coords = feature.geometry.coordinates;
      if (feature.geometry.type === 'Polygon') {
        coords[0].forEach((coord: number[]) => {
          minX = Math.min(minX, coord[0]);
          minY = Math.min(minY, coord[1]);
          maxX = Math.max(maxX, coord[0]);
          maxY = Math.max(maxY, coord[1]);
        });
      }
    });

    return isFinite(minX) ? [minX, minY, maxX, maxY] : null;
  };

  const decodeUint8Array = (value: any): string => {
    if (value instanceof Uint8Array) {
      return new TextDecoder().decode(value);
    }
    return String(value);
  };

  const processParquetData = (rows: any[]) => {
    if (!Array.isArray(rows)) {
      console.error('Invalid data format:', rows);
      return { type: 'FeatureCollection', features: [] };
    }

    const features = extractFeatures(rows);
    
    return {
      type: 'FeatureCollection',
      features
    };
  };

  const extractFeatures = (rows: any[]) => {
    return rows.map(row => {
      try {
        // Parse WKB geometry
        let geometry = null;
        if (row.geometry) {
          if (row.geometry instanceof Uint8Array) {
            const wkbBuffer = Buffer.from(row.geometry);
            geometry = wkx.Geometry.parse(wkbBuffer).toGeoJSON();
          } else if (typeof row.geometry === 'string') {
            const buffer = Buffer.from(row.geometry, 'binary');
            geometry = wkx.Geometry.parse(buffer).toGeoJSON();
          }
        }

        if (!geometry) {
          console.warn('No valid geometry found in row:', row);
          return null;
        }

        // Decode text fields
        const properties = Object.fromEntries(
          Object.entries(row)
            .filter(([key]) => key !== 'geometry')
            .map(([key, value]) => [key, decodeUint8Array(value)])
        );

        return {
          type: 'Feature',
          geometry,
          properties
        };
      } catch (err) {
        console.warn('Error processing feature:', err);
        return null;
      }
    }).filter(Boolean);
  };

  if (error) {
    return <div>Error: {error.message}</div>;
  }

  const layers = data ? [
    new GeoJsonLayer({
      id: 'parquet-layer',
      data,
      pickable: true,
      stroked: true,
      filled: true,
      extruded: false,
      lineWidthScale: 1,
      lineWidthMinPixels: 1,
      getFillColor: [160, 160, 180, 100],
      getLineColor: [80, 80, 80, 255],
      lineWidthUnits: 'pixels',
      getLineWidth: 1,
      ...layerProps
    })
  ] : [];

  return (
    <DeckGL
      ref={deckRef}
      initialViewState={viewState}
      controller={true}
      layers={layers}
      onViewStateChange={({viewState}) => setViewState(viewState)}
    >
      <Map
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
      />
    </DeckGL>
  );
};

export default ParquetLayer;
