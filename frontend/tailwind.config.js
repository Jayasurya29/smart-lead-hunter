/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['DM Sans', 'system-ui', 'sans-serif'],
      },
      colors: {
        navy: {
          50: '#e8edf5', 100: '#c5cfdf', 200: '#9fb0c8', 300: '#7891b0',
          400: '#5b7a9e', 500: '#3e638c', 600: '#2e4a6e', 700: '#253d5e',
          800: '#1a2d4a', 900: '#0f1d32', 950: '#0a1628',
        },
        stone: {
          50: '#faf9f7', 100: '#f3f1ed', 200: '#e8e4dd', 300: '#d4cfc5', 400: '#b0a99e',
        },
        gold: {
          50: '#fdf8eb', 100: '#f9edcb', 200: '#f0d98f', 300: '#e5c254',
          400: '#d4a853', 500: '#c49a3c', 600: '#a8832e',
        },
        coral: {
          50: '#fdf2f0', 100: '#fce0dc', 200: '#f8b9b0',
          400: '#ef7d6d', 500: '#e85d4a', 600: '#d14836',
        },
      },
    },
  },
  plugins: [],
}
