/** @type {import('next').NextConfig} */
const { version } = require("./package.json");
const nextConfig = {
	reactStrictMode: true,
	trailingSlash: true,
	images: {
		remotePatterns: [
			{
				protocol: "https",
				hostname: "*",
				port: "",
				pathname: "/**",
			},
		],
	},
	publicRuntimeConfig: {
		version,
	},
	webpack: (config, { isServer }) => {
		// Handle fs module in browser
		if (!isServer) {
			config.resolve.fallback = {
				...config.resolve.fallback,
				fs: false,
			};
		}
		return config;
	},
};

module.exports = nextConfig;
