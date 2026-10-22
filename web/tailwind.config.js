/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./app/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["var(--font-sans)", "ui-sans-serif", "system-ui"],
        display: ["var(--font-display)", "ui-serif", "Georgia"],
        mono: ["var(--font-mono)", "ui-monospace", "monospace"],
      },
      colors: {
        ink: "#1c1915",
        paper: "#f3ecdf",
        card: "#fffaf3",
        line: "#e2d5c3",
        signal: "#b8431f",
        pine: "#1f6b4a",
      },
    },
  },
  plugins: [],
};
