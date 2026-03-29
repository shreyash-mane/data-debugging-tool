/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      fontFamily: {
        mono: ['"JetBrains Mono"', 'ui-monospace', 'monospace'],
        sans: ['"IBM Plex Sans"', 'system-ui', 'sans-serif'],
      },
      colors: {
        surface: {
          0: '#0f1117',
          1: '#161b27',
          2: '#1e2535',
          3: '#252d40',
          4: '#2d364d',
        },
        accent: {
          DEFAULT: '#3b82f6',
          hover: '#2563eb',
        },
        danger: '#ef4444',
        warn: '#f59e0b',
        ok: '#22c55e',
        muted: '#6b7280',
      },
    },
  },
  plugins: [],
}
