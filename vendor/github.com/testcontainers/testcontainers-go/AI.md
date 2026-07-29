# AI Coding Agent Guidelines

This document provides guidelines for AI coding agents working on the Testcontainers for Go repository.

## Repository Overview

This is a **Go monorepo** containing:
- **Core library**: Root directory contains the main testcontainers-go library
- **Modules**: `./modules/` directory with 50+ technology-specific modules (postgres, redis, kafka, etc.)
- **Examples**: `./examples/` directory with example implementations
- **Module generator**: `./modulegen/` directory with tools to generate new modules
- **Documentation**: `./docs/` directory with MkDocs-based documentation

## Environment Setup

### Go Version
- **Required**: Go 1.25.9
- **Tool**: Use [gvm](https://github.com/andrewkroh/gvm) for version management
- **CRITICAL**: Always run this before ANY Go command:
  ```bash
  # For Apple Silicon (M1/M2/M3)
  eval "$(gvm 1.25.9 --arch=arm64)"

  # For Intel/AMD (x86_64)
  eval "$(gvm 1.25.9 --arch=amd64)"
  ```

### Project Structure
Each module in `./modules/` is a separate Go module with:
- `go.mod` / `go.sum` - Module dependencies
- `{module}.go` - Main module implementation
- `{module}_test.go` - Unit tests
- `examples_test.go` - Testable examples for documentation
- `Makefile` - Standard targets: `pre-commit`, `test-unit`

## Development Workflow

### Before Making Changes
1. **Read existing code** in similar modules for patterns
2. **Check documentation** in `docs/modules/index.md` for best practices
3. **Run tests** to ensure baseline passes

### Working with Modules
1. **Change to module directory**: `cd modules/{module-name}`
2. **Run pre-commit checks**: `make pre-commit` (linting, formatting, tidy)
3. **Run tests**: `make test-unit`
4. **Both together**: `make pre-commit test-unit`

### Git Workflow
- **Branch naming**: Use descriptive names like `chore-module-use-run`, `feat-add-xyz`, `fix-module-issue`
  - **NEVER** use `main` branch for PRs (they will be auto-closed)
- **Commit format**: Conventional commits (enforced by CI)
  ```text
  type(scope): description

  Longer explanation if needed.

  🤖 Generated with [Claude Code](https://claude.com/claude-code)

  Co-Authored-By: Claude <noreply@anthropic.com>
  ```
- **Commit types** (enforced): `security`, `fix`, `feat`, `docs`, `chore`, `deps`
- **Scope rules**:
  - Optional (can be omitted for repo-level changes)
  - Must be lowercase (uppercase scopes are rejected)
  - Examples: `feat(redis)`, `chore(kafka)`, `docs`, `fix(postgres)`
- **Subject rules**:
  - Must NOT start with uppercase letter
  - ✅ Good: `feat(redis): add support for clustering`
  - ❌ Bad: `feat(redis): Add support for clustering`
- **Breaking changes**: Add `!` after type: `feat(redis)!: remove deprecated API`
- **Always include co-author footer** when AI assists with changes

### Pull Requests
- **Title format**: Same as commit format (validated by CI)
  - `type(scope): description`
  - Examples: `feat(redis): add clustering support`, `docs: improve module guide`, `chore(kafka): update tests`
- **Title validation** enforced by `.github/workflows/conventions.yml`
- **Labels**: Use appropriate labels (`chore`, `breaking change`, `documentation`, etc.)
- **Body template**:
  ```markdown
  ## What does this PR do?

  Brief description of changes.

  ## Why is it important?

  Context and rationale.

  ## Related issues

  - Relates to #issue-number
  ```

## Module Development Best Practices

**📖 Detailed guide**: See [`docs/modules/index.md`](docs/modules/index.md) for comprehensive module development documentation.

### Quick Reference

#### Container Struct
- **Name**: Use `Container`, not module-specific names like `PostgresContainer`
- **Fields**: Use private fields for state management
- **Embedding**: Always embed `testcontainers.Container`

```go
type Container struct {
    testcontainers.Container
    dbName   string  // private
    user     string  // private
}
```

#### Run Function Pattern
Five-step implementation:
1. Process custom options (if using intermediate settings)
2. Build `moduleOpts` with defaults
3. Add conditional options based on settings
4. Append user options (allows overrides)
5. Call `testcontainers.Run` and return with proper error wrapping

```go
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
    // See docs/modules/index.md for complete implementation
    moduleOpts := []testcontainers.ContainerCustomizer{
        testcontainers.WithExposedPorts("5432/tcp"),
        // ... defaults
    }
    moduleOpts = append(moduleOpts, opts...)

    ctr, err := testcontainers.Run(ctx, img, moduleOpts...)
    if err != nil {
        return nil, fmt.Errorf("run modulename: %w", err)
    }
    return &Container{Container: ctr}, nil
}
```

#### Container Options
- **Simple config**: Use built-in `testcontainers.With*` options
- **Complex logic**: Use `testcontainers.CustomizeRequestOption`
- **State transfer**: Create custom `Option` type

**Critical rules:**
- ✅ Return struct types (not interfaces)
- ✅ Call built-in options directly: `testcontainers.WithFiles(f)(req)`
- ❌ Don't use `.Customize()` method
- ❌ Don't pass slices to variadic functions

#### Common Patterns
- **Env inspection**: Use `strings.CutPrefix` with early exit
- **Variadic args**: Pass directly, not as slices
- **Option order**: defaults → user options → post-processing
- **Error format**: `fmt.Errorf("run modulename: %w", err)`

**For complete examples and detailed explanations**, see [`docs/modules/index.md`](docs/modules/index.md).

## Testing Guidelines

### Running Tests
- **From module directory**: `cd modules/{module} && make test-unit`
- **Pre-commit checks**: `make pre-commit` (run this first to catch lint issues)
- **Full check**: `make pre-commit test-unit`

### Test Patterns
- Use testable examples in `examples_test.go`
- Follow existing test patterns in similar modules
- Test both success and error cases
- Use `t.Parallel()` when tests are independent

### When Tests Fail
1. **Read the error message carefully** - it usually tells you exactly what's wrong
2. **Check if it's a lint issue** - run `make pre-commit` first
3. **Verify Go version** - ensure using Go 1.25.9
4. **Check Docker** - some tests require Docker daemon running

## Common Pitfalls to Avoid

### Code Issues
- ❌ Using interface types as return values
- ❌ Forgetting to run `eval "$(gvm 1.25.9 --arch=arm64)"`
- ❌ Not handling errors from built-in options
- ❌ Using module-specific container names (`PostgresContainer`)
- ❌ Calling `.Customize()` method instead of direct function call

### Git Issues
- ❌ Forgetting co-author footer in commits
- ❌ Not running tests before committing
- ❌ Committing files outside module scope (use `git add modules/{module}/`)
- ❌ Using uppercase in scope: `feat(Redis)` → use `feat(redis)`
- ❌ Starting subject with uppercase: `fix: Add feature` → use `fix: add feature`
- ❌ Using wrong commit type (only: `security`, `fix`, `feat`, `docs`, `chore`, `deps`)
- ❌ Creating PR from `main` branch (will be auto-closed)

### Testing Issues
- ❌ Running tests without pre-commit checks first
- ❌ Not changing to module directory before running make
- ❌ Forgetting to set Go version before testing

## Reference Documentation

For detailed information, see:
- **Module development**: `docs/modules/index.md` - Comprehensive best practices
- **Contributing**: `docs/contributing.md` - General contribution guidelines
- **Modules catalog**: [testcontainers.com/modules](https://testcontainers.com/modules/?language=go)
- **API docs**: [pkg.go.dev/github.com/testcontainers/testcontainers-go](https://pkg.go.dev/github.com/testcontainers/testcontainers-go)

## Module Generator

To create a new module:

```bash
cd modulegen
go run . new module --name mymodule --image "docker.io/myimage:tag"
```

This generates:
- Module scaffolding with proper structure
- Documentation template
- Test files with examples
- Makefile with standard targets

The generator uses templates in `modulegen/_template/` that follow current best practices.

## Need Help?

- **Slack**: [testcontainers.slack.com](https://slack.testcontainers.org/)
- **GitHub Discussions**: [github.com/testcontainers/testcontainers-go/discussions](https://github.com/testcontainers/testcontainers-go/discussions)
- **Issues**: Check existing issues or create a new one with detailed context
