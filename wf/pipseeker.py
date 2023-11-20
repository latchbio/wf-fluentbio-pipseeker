import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional

from latch import medium_task, custom_task
from latch.types import LatchDir, LatchFile, LatchOutputDir
import sys

sys.stdout.reconfigure(line_buffering=True)


class Chemistry(Enum):
    v3 = "v3"
    v4 = "v4"


# class Verbosity(Enum):
#     zero = "0"
#     one = "1"
#     two = "2"


@custom_task(cpu=18, memory=60)
def pipseeker_task(
    fastqs: LatchDir,
    output_directory: LatchOutputDir,
    chemistry: Chemistry = Chemistry.v4,
    sorted_bam: bool = False,
    # run_barnyard: bool = False,
    # verbosity: Optional[Verbosity] = None,
) -> LatchOutputDir:
    print()
    print("Downloading default reference pipseeker-gex-reference-GRCh38-2022.04.tar.gz")
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

    print()
    print("Preparing run")
    local_output_dir = Path("/root/pipseeker_out")

    pipseeker_cmd = [
        "pipseeker",
        "full",
        "--fastq",
        f"{fastqs.local_path}/.",  # needs dot at the end
        "--star-index-path",
        f"{reference_p}",
        "--output-path",
        str(local_output_dir),
        "--threads",
        "16",
        "--chemistry",
        str(chemistry.value),
        "--verbosity",
        "2",
        "--skip-version-check",
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

    # if run_barnyard is True:
    #     pipseeker_cmd.append("--run-barnyard")

    if sorted_bam is True:
        pipseeker_cmd.append("--sorted-bam")

    # if verbosity is not None:
    #     pipseeker_cmd.extend(
    #         [
    #             "--verbosity",
    #             str(verbosity.value),
    #         ]
    #     )

    print()
    print(f'Running {" ".join(pipseeker_cmd)}')
    subprocess.run(pipseeker_cmd, check=True)

    print()
    print("Uploading results")
    return LatchOutputDir(str(local_output_dir), output_directory.remote_path)
