from pathlib import Path

ROOT = Path("/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline")
RAW = ROOT / "data" / "raw"

MAX_CNPJS = 3000

ESTAB_SOURCE = RAW / "K3241.K03200Y0.D60314.ESTABELE"
EMP_SOURCE = RAW / "K3241.K03200Y0.D60314.EMPRECSV"
SOCIO_SOURCE = RAW / "K3241.K03200Y0.D60314.SOCIOCSV"
SIMPLES_SOURCE = RAW / "F.K03200$W.SIMPLES.CSV.D60314"

ESTAB_TARGET = RAW / "estabelecimentos_sample.csv"
EMP_TARGET = RAW / "empresas_sample.csv"
SOCIO_TARGET = RAW / "socios_sample.csv"
SIMPLES_TARGET = RAW / "simples_sample.csv"


def get_cnpj_basico(line: str) -> str:
    return line.split(";")[0].strip().strip('"')


def collect_cnpjs_from_estabelecimentos(limit: int) -> set[str]:
    selected = []
    seen = set()

    with ESTAB_SOURCE.open("r", encoding="latin-1") as f:
        for line in f:
            cnpj = get_cnpj_basico(line)
            if cnpj not in seen:
                seen.add(cnpj)
                selected.append(cnpj)
            if len(selected) >= limit:
                break

    return set(selected)


def filter_file(source: Path, target: Path, selected_cnpjs: set[str]) -> int:
    written = 0
    with source.open("r", encoding="latin-1") as src, target.open("w", encoding="latin-1") as dst:
        for line in src:
            cnpj = get_cnpj_basico(line)
            if cnpj in selected_cnpjs:
                dst.write(line)
                written += 1
    return written


def main():
    selected_cnpjs = collect_cnpjs_from_estabelecimentos(MAX_CNPJS)

    estab_count = filter_file(ESTAB_SOURCE, ESTAB_TARGET, selected_cnpjs)
    emp_count = filter_file(EMP_SOURCE, EMP_TARGET, selected_cnpjs)
    socio_count = filter_file(SOCIO_SOURCE, SOCIO_TARGET, selected_cnpjs)
    simples_count = filter_file(SIMPLES_SOURCE, SIMPLES_TARGET, selected_cnpjs)

    print(f"cnpjs selecionados: {len(selected_cnpjs)}")
    print(f"estabelecimentos_sample.csv: {estab_count}")
    print(f"empresas_sample.csv: {emp_count}")
    print(f"socios_sample.csv: {socio_count}")
    print(f"simples_sample.csv: {simples_count}")


if __name__ == "__main__":
    main()
