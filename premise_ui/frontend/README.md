# Premise UI Frontend

This directory contains the React and Vite source frontend for `premise_ui`.

## Commands

Install dependencies:

```bash
npm install
```

Start the frontend dev server:

```bash
npm run dev
```

Build production assets into `premise_ui/frontend/dist`:

```bash
npm run build
```

Run the Scenario Explorer helper benchmark against a synthetic large summary:

```bash
npm run benchmark:explorer -- --scenarios 8 --groups 16 --variables 10 --iterations 25
```

Run the checked-in benchmark smoke check with the baseline options and ceilings:

```bash
npm run benchmark:explorer:check
```

The current Python launcher still serves the checked-in `dist/` bundle. The
source app in `src/` is the replacement frontend under development.
