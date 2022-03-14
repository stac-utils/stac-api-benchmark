"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """STAC API Benchmark."""


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
