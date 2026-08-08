# Polaris docs

This directory contains the source for the Polaris documentation site at [docs.polaris.supply](https://docs.polaris.supply).

If you want to use Polaris, start with the live docs:

- [docs.polaris.supply](https://docs.polaris.supply)

## Local development

Install the Mintlify CLI:

```bash
npm i -g mint
```

Run the docs locally from the documentation directory:

```bash
cd docs
mint dev
```

The local preview runs at `http://localhost:3000`.

## Project structure

- `docs.json` contains site-wide configuration and navigation.
- `*.mdx` files contain documentation pages.
- `images/` contains static assets.

## Publishing

Changes deployed from the default branch update the production docs site automatically.

## Writing notes

- Use active voice and second person.
- Keep sentences concise.
- Format UI labels in bold.
- Format commands, paths, and filenames in code style.
