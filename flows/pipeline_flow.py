from pathlib import Path
import subprocess

from prefect import flow, task


ROOT_DIR = Path("/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline")
RAW_DIR = ROOT_DIR / "data" / "raw"


@task(log_prints=True)
def generate_samples():
    sample_specs = [
        ("K3241.K03200Y0.D60314.EMPRECSV", "empresas_sample.csv", 10000),
        ("K3241.K03200Y0.D60314.ESTABELE", "estabelecimentos_sample.csv", 10000),
        ("K3241.K03200Y0.D60314.SOCIOCSV", "socios_sample.csv", 10000),
        ("F.K03200$W.SIMPLES.CSV.D60314", "simples_sample.csv", 10000),
    ]

    full_copy_specs = [
        ("F.K03200$Z.D60314.CNAECSV", "cnaes.csv"),
        ("F.K03200$Z.D60314.MUNICCSV", "municipios.csv"),
        ("F.K03200$Z.D60314.NATJUCSV", "naturezas.csv"),
        ("F.K03200$Z.D60314.QUALSCSV", "qualificacoes.csv"),
    ]

    for source_name, target_name, limit in sample_specs:
        source_path = RAW_DIR / source_name
        target_path = RAW_DIR / target_name

        with source_path.open("r", encoding="latin-1") as src, target_path.open("w", encoding="latin-1") as dst:
            for index, line in enumerate(src):
                if index >= limit:
                    break
                dst.write(line)

        print(f"sample generated: {target_path.name}")

    for source_name, target_name in full_copy_specs:
        source_path = RAW_DIR / source_name
        target_path = RAW_DIR / target_name
        target_path.write_text(source_path.read_text(encoding="latin-1"), encoding="latin-1")
        print(f"reference copied: {target_path.name}")


@task(log_prints=True)
def run_command(command: list[str]):
    result = subprocess.run(
        command,
        cwd=ROOT_DIR,
        text=True,
        capture_output=True,
        check=False,
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")

    return result.stdout


@flow(name="cnpj-lakehouse-pipeline")
def cnpj_pipeline_flow():
    generate_samples()
    run_command(["dbt", "run"])
    run_command(["dbt", "test"])


if __name__ == "__main__":
    cnpj_pipeline_flow()
