// This file exists only to mark frontend/ as a Go module boundary so the
// root module's `./...` patterns (go build/vet/test) don't descend into
// frontend/node_modules, which ships stray Go source in some npm packages.
// frontend/ has no actual Go code — it's a Next.js app.
module boilerpulse/frontend-placeholder

go 1.25
