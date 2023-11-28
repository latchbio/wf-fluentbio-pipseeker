import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional

from latch import medium_task, custom_task
from latch.types import LatchDir, LatchFile, LatchOutputDir
import sys

sys.stdout.reconfigure(line_buffering=True)


class GenomeType(Enum):
    human = "Human"
    mouse = "Mouse"


class Chemistry(Enum):
    v3 = "v3"
    v4 = "v4"


class Verbosity(Enum):
    zero = "0"
    one = "1"
    two = "2"


@custom_task(cpu=18, memory=60)
def pipseeker_task(
    fastq_directory: LatchDir,
    genome_source: str,
    compiled_genome_reference: GenomeType,
    custom_genome_reference_fasta: LatchFile,
    custom_genome_reference_gtf: LatchFile,
    chemistry: Chemistry = Chemistry.v4,
    output_directory: LatchOutputDir = LatchOutputDir("latch:///PIPseeker_Output"),
    verbosity: Verbosity = Verbosity.two,
    random_seed: int = 0,
    save_svg: bool = True,
    dpi: int = 200,
    sorted_bam: bool = False,
    # run_barnyard: bool = False,
    remove_bam: bool = True,
    downsample: Optional[int] = None,
    retain_barcoded_fastqs: bool = False,
    exons_only: bool = False,
    min_sensitivity: int = 1,
) -> LatchOutputDir:
    print()
    print("Compiling reference genome")

    if genome_source == "compiled":
        if compiled_genome_reference == GenomeType.human:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-GRCh38-2022.04.tar.gz"
            ).local_path
            reference_p = Path("/root/pipseeker-gex-reference-GRCh38-2022.04")

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif compiled_genome_reference == GenomeType.mouse:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-GRCm39-2022.04.tar.gz"
            ).local_path  # Not uploaded to my test-data yet
            reference_p = Path("/root/pipseeker-gex-reference-GRCm39-2022.04")

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    elif genome_source == "custom":
        custom_genome_reference_gtf_p = Path(custom_genome_reference_gtf)
        custom_genome_reference_fasta_p = Path(custom_genome_reference_fasta)
        reference_p = Path("/root/genome_ref")

        genome_compilation_cmd = [
            "pipseeker",
            "buildmapref",
            "--fasta",
            f"{custom_genome_reference_fasta_p}",
            "--gtf",
            f"{custom_genome_reference_gtf_p}",
            "--output-path",
            f"{reference_p}",
        ]  # Need more parameters here?
        subprocess.run(genome_compilation_cmd, check=True)

    print()
    print("Preparing run")
    local_output_dir = Path("/root/pipseeker_out")

    pipseeker_cmd = [
        "pipseeker",
        "full",
        "--fastq",
        f"{fastq_directory.local_path}/.",  # needs dot at the end
        "--star-index-path",
        f"{reference_p}",
        "--output-path",
        str(local_output_dir),
        "--threads",
        "16",
        "--chemistry",
        str(chemistry.value),
        "--verbosity",
        f"{verbosity.value}",
        "--skip-version-check",
        "--random_seed",
        f"{random_seed}",
        "--save-svg",
        f"{save_svg}",
        "--dpi",
        f"{dpi}",
        "--min-sensitivity",
        f"{min_sensitivity}",
    ]

    if chemistry == Chemistry.v3:
        pipseeker_cmd.extend(
            [
                "--annotation",
                "/root/ref/human-pbmc-references/references/human-pbmc-v3.csv",  # fixed
            ]
        )
    else:
        pipseeker_cmd.extend(
            [
                "--annotation",
                "/root/ref/human-pbmc-references/references/human-pbmc-v4.csv",  # fixed
            ]
        )

    if downsample is not None:
        pipseeker_cmd.extend(
            [
                "--downsample-to",
                f"{downsample}",
            ]
        )

    if retain_barcoded_fastqs is True:
        pipseeker_cmd.append("--retain-barcoded-fastqs")

    if sorted_bam is True:
        pipseeker_cmd.append("--sorted-bam")

    if remove_bam is True:
        pipseeker_cmd.append("--remove-bam")

    if exons_only is True:
        pipseeker_cmd.append("--exons-only")

    # if run_barnyard is True:
    #     pipseeker_cmd.append("--run-barnyard")

    print()
    print(f'Running {" ".join(pipseeker_cmd)}')
    subprocess.run(pipseeker_cmd, check=True)

    print()
    print("Uploading results")
    return LatchOutputDir(str(local_output_dir), output_directory.remote_path)
