import subprocess
import sys

from enum import Enum
from pathlib import Path
from typing import Optional

from latch import custom_task
from latch.functions.messages import message
from latch.types import LatchDir, LatchFile, LatchOutputDir

sys.stdout.reconfigure(line_buffering=True)


class GenomeType(Enum):
    human = "Human"
    mouse = "Mouse"
    human_mouse = "Human and Mouse"
    drosophilia = "Drosophilia"
    zebrafish = "Zebrafish"
    arabidopsis_thaliana = "Arabidopsis thaliana"


class Chemistry(Enum):
    v3 = "v3"
    v4 = "v4"
    v5 = "v5"

class Verbosity(Enum):
    zero = "0"
    one = "1"
    two = "2"


def get_prebuilt_mappping_reference(*, genome_source, prebuilt_genome, custom_prebuilt_genome,
                                    custom_prebuilt_genome_zipped):
    """
    Download and unpack prebuilt mapping references (using Fluent-built or custom).
    Returns:
        reference_p: Path to the prebuilt mapping reference.
    """
    print("\nPreparing reference genome")

    if genome_source == "prebuilt_genome":
        if prebuilt_genome == GenomeType.human:
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
        elif prebuilt_genome == GenomeType.mouse:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-GRCm39-2022.04.tar.gz"
            ).local_path
            reference_p = Path("/root/pipseeker-gex-reference-GRCm39-2022.04")

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif prebuilt_genome == GenomeType.human_mouse:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-GRCh38-and-GRCm39-2022.04.tar.gz"
            ).local_path
            reference_p = Path(
                "/root/pipseeker-gex-reference-GRCh38-and-GRCm39-2022.04"
            )

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif prebuilt_genome == GenomeType.drosophilia:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-dm-flybase-r6-v47-2022.09.tar.gz"
            ).local_path
            reference_p = Path(
                "/root/pipseeker-gex-reference-dm-flybase-r6-v47-2022.09"
            )

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif prebuilt_genome == GenomeType.zebrafish:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/zebrafish_danio_rerio_GRCz11_r110_2023.08.tar.gz"
            ).local_path
            reference_p = Path("/root/zebrafish_danio_rerio_GRCz11_r110_2023.08")

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif prebuilt_genome == GenomeType.arabidopsis_thaliana:
            reference_zipped_p = LatchFile(
                "s3://latch-public/test-data/18440/pipseeker-gex-reference-arabidopsis-thaliana-TAIR10.55-protein-coding-2023.02.tar.gz"
            ).local_path
            reference_p = Path(
                "/root/pipseeker-gex-reference-arabidopsis-thaliana-TAIR10.55-protein-coding-2023.02"
            )

            subprocess.run(
                ["tar", "-zxvf", f"{reference_zipped_p}", "-C", "/root"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    # Using a custom prebuilt mapping reference.
    elif genome_source == "custom_prebuilt_genome":
        if custom_prebuilt_genome is not None:
            reference_p = Path(custom_prebuilt_genome)
        elif custom_prebuilt_genome_zipped is not None:
            print("Unpacking the custom prebuilt genome")
            reference_zipped_p = Path(custom_prebuilt_genome_zipped)
            reference_p = Path(f"/root/{reference_zipped_p.stem}").with_suffix("")

            unpacked_data = False # Tracks whether the untar/unzip operation was attempted.
            if reference_zipped_p.suffixes[-2:] == [".tar", ".gz"]:
                subprocess.run(
                    ["tar", "-zxvf", str(reference_zipped_p), "-C", "/root"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                unpacked_data = True

            elif reference_zipped_p.suffix == ".zip":
                subprocess.run(
                    ["unzip", "-o", str(reference_zipped_p), "-d", "/root"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                unpacked_data = True

            # Check whether the uncompressed directory exists and matches the expected name.
            if unpacked_data:
                #   This handles the case where users might not pack their data inside of a single directory
                #   or the top-level zipped dir might use a different name than the parent zipped filename.
                #   If the directory is not found, the user will be prompted to re-upload the data.
                # List the files in the root dir and see if the directory name is present from the zipped file output
                root_dir = Path("/root")
                root_dir_files = [f.name for f in root_dir.iterdir()]

                # Check if the directory name is present in the root dir
                if not reference_zipped_p.stem in root_dir_files:
                    raise ValueError(f"Unpacking failed. The directory {reference_p} was not found.\n"
                                     "Please ensure that you compressed your reference with a single top-level "
                                     "directory containing the reference genome. \n"
                                     "Also ensure the top-level folder matches the prefix of your compressed file.")

            else:
                # Data was not unpacked, due to file extension mismatch.
                print('The provided genome must be compressed using .tar.gz or .zip format '
                      'or uploaded without compression.')
    else:
        print("No reference genome provided. Continuing.")
        print(f"Genome source: {genome_source}")
        reference_p = None

    return reference_p


@custom_task(cpu=18, memory=190, storage_gib=500)
#   Above is for T20 build.  For T2 mode -->  @custom_task(cpu=8, memory=64, storage_gib=300)
def pipseeker_task(*,
                   pipseeker_mode: str,
                   output_directory: LatchOutputDir = LatchOutputDir("latch:///PIPseeker_Output"),
                   fastq_directory: Optional[LatchDir] = None,
                   chemistry: Chemistry = Chemistry.v4,
                   genome_source: str,
                   prebuilt_genome: GenomeType,
                   custom_prebuilt_genome: Optional[LatchDir],
                   custom_prebuilt_genome_zipped: Optional[LatchFile],
                   verbosity: Verbosity = Verbosity.two,
                   random_seed: int = 0,
                   save_svg: bool = False,
                   dpi: int = 200,
                   downsample_to: Optional[int] = None,
                   input_reads: Optional[int] = None,
                   retain_barcoded_fastqs: bool = False,
                   sorted_bam: bool = False,
                   remove_bam: bool = False,
                   exons_only: bool = False,
                   min_sensitivity: int = 1,
                   max_sensitivity: int = 5,
                   force_cells: Optional[int] = None,
                   run_barnyard: bool = False,
                   clustering_percent_genes: int = 10,
                   diff_exp_genes: int = 50,
                   principal_components: Optional[int] = None,
                   nearest_neighbors: Optional[int] = None,
                   resolution: Optional[int] = None,
                   clustering_sensitivity: str = "medium",
                   min_clusters_kmeans: Optional[int] = None,
                   max_clusters_kmeans: Optional[int] = None,
                   umap_axes: bool = False,
                   annotation: Optional[LatchFile] = None,
                   report_id: Optional[str] = None,
                   report_description: Optional[str] = None,
                   snt_fastq: Optional[LatchFile] = None,
                   snt_tags: Optional[LatchFile] = None,
                   snt_position: int = 0,
                   snt_annotation: Optional[LatchFile] = None,
                   snt_colormap: str = "gray-to-green",
                   snt_min_percent: int = 1,
                   snt_max_percent: int = 99,
                   snt_min_value: Optional[int] = None,
                   snt_max_value: Optional[int] = None,
                   hto_fastq: Optional[LatchFile] = None,
                   hto_tags: Optional[LatchFile] = None,
                   hto_position: int = 0,
                   hto_annotation: Optional[LatchFile] = None,
                   hto_colormap: str = "gray-to-red",
                   hto_colorbar: bool = False,
                   hto_min_percent: int = 1,
                   hto_max_percent: int = 99,
                   hto_min_value: Optional[int] = None,
                   hto_max_value: Optional[int] = None,

                   # cells mode args
                   previous: Optional[LatchDir] = None,
                   hash_cellsmode: Optional[str] = None,

                   # buildmapref mode args
                   custom_genome_reference_fasta: Optional[LatchFile] = None,
                   custom_genome_reference_gtf: Optional[LatchFile] = None,
                   include_types: Optional[str] = None,
                   exclude_types: Optional[str] = None,
                   biotype_tag: Optional[str] = None,
                   read_length: Optional[int] = 100,
                   sparsity: Optional[int] = 3,
                   additional_params_buildmapref: Optional[str] = None
                   ) -> LatchOutputDir:

    print(f"Running {pipseeker_mode}")

    # Shared args.
    universal_shared_args = [
        "--threads",
        "0",
        "--verbosity",
        f"{verbosity.value}",
        "--skip-version-check"
    ]

    if pipseeker_mode in ['full_mode', 'cells_mode']:

        print("\nPreparing run")

        # Shared args for full and cells mode.
        full_and_cells_shared_args = [
            "--random-seed",
            f"{random_seed}",
            "--dpi",
            f"{dpi}",
            "--min-sensitivity",
            f"{min_sensitivity}",
            "--max-sensitivity",
            f"{max_sensitivity}",
            "--clustering-percent-genes",
            f"{clustering_percent_genes}",
            "--diff-exp-genes",
            f"{diff_exp_genes}",
            "--clustering-sensitivity",
            f"{clustering_sensitivity}"
        ]

        # Full Mode.
        if pipseeker_mode == 'full_mode':
            # Obtain the reference path for prebuilt references.
            reference_p = get_prebuilt_mappping_reference(genome_source=genome_source,
                                                          prebuilt_genome=prebuilt_genome,
                                                          custom_prebuilt_genome=custom_prebuilt_genome,
                                                          custom_prebuilt_genome_zipped=custom_prebuilt_genome_zipped)

            # Define the local path.
            local_output_dir = Path("/root/pipseeker_out")
            # Define target directory for the output.
            destination_directory = output_directory.remote_path

            pipseeker_cmd = [
                "pipseeker",
                "full",
                "--fastq",
                f"{fastq_directory.local_path}/.",  # Use period for directory input.
                "--star-index-path",
                f"{reference_p}",
                "--chemistry",
                f"{chemistry.value}",
                "--output-path",
                f"{local_output_dir}",
            ]

            pipseeker_cmd += universal_shared_args + full_and_cells_shared_args

            # Add full mode specific optional vars.
            if downsample_to is not None:
                pipseeker_cmd += [
                    "--downsample-to",
                    f"{downsample_to}"
                ]
                if input_reads is not None:
                    pipseeker_cmd += [
                        "--input-reads",
                        f"{input_reads}"
                    ]

        # Cells Mode.
        elif pipseeker_mode == 'cells_mode':

            # Define the local and target path for full and cells mode.
            local_output_dir = previous.local_path
            destination_directory = previous.remote_path

            pipseeker_cmd = [
                "pipseeker",
                "cells",
                "--previous",
                f"{local_output_dir}"
            ]
            pipseeker_cmd += universal_shared_args + full_and_cells_shared_args

            # Add cells mode specific optional vars.
            if hash_cellsmode is not None:
                pipseeker_cmd += [
                    "--hash",
                    f"{hash_cellsmode}"
                ]

        # Extend Full or Cells mode commands.
        if force_cells is not None:
            pipseeker_cmd += [
                "--force-cells",
                f"{force_cells}"
            ]

        if min_clusters_kmeans is not None:
            pipseeker_cmd += [
                "--min-clusters-kmeans",
                f"{min_clusters_kmeans}"
            ]

        if max_clusters_kmeans is not None:
            pipseeker_cmd += [
                "--max-clusters-kmeans",
                f"{max_clusters_kmeans}"
            ]

        if annotation is not None:
            pipseeker_cmd += [
                "--annotation",
                f"{annotation.local_path}"
            ]

        if report_id is not None:
            pipseeker_cmd += [
                "--id",
                f"{report_id}"
            ]

        if report_description is not None:
            pipseeker_cmd += [
                "--description",
                f"{report_description}"
            ]

        if save_svg is True:
            pipseeker_cmd.append("--save-svg")

        if retain_barcoded_fastqs is True:
            pipseeker_cmd.append("--retain-barcoded-fastqs")

        if sorted_bam is True:
            pipseeker_cmd.append("--sorted-bam")

        if remove_bam is True:
            pipseeker_cmd.append("--remove-bam")

        if exons_only is True:
            pipseeker_cmd.append("--exons-only")

        if run_barnyard is True:
            pipseeker_cmd.append("--run-barnyard")

        if umap_axes is True:
            pipseeker_cmd.append("--umap-axes")

        parameters = [principal_components, nearest_neighbors, resolution]

        if all(param is None for param in parameters) or all(
                param is not None for param in parameters
        ):
            if all(param is not None for param in parameters):
                pipseeker_cmd += [
                    "--principal-components",
                    f"{principal_components}",
                    "--nearest-neighbors",
                    f"{nearest_neighbors}",
                    "--resolution",
                    f"{resolution}",
                ]
        else:
            message(
                typ="warning",
                data={
                    "title": "PIPseeker parameters warning",
                    "body": "--principal-components, --nearest-neighbors, and --resolution must all be used or omitted at the same time. "
                            "You cannot specify one argument and leave the others unspecified. "
                            "PIPseeker will run with none of the inputted values and assign these parameters automatically.",
                },
            )

        if snt_fastq is not None:
            pipseeker_cmd += [
                "--snt-fastq",
                f"{snt_fastq.local_path}/.",  # Use period for directory input.
                "--snt-position",
                f"{snt_position}"
            ]

            if snt_tags is not None:
                pipseeker_cmd += [
                    "--snt-tags",
                    f"{snt_tags.local_path}",
                ]

            if snt_annotation is not None:
                pipseeker_cmd += [
                    "--snt-annotation",
                    f"{snt_annotation.local_path}",
                ]

            if snt_colormap is not None:
                pipseeker_cmd += [
                    "--snt-colormap",
                    f"{snt_colormap}"
                ]

            if (snt_min_value is not None) and (snt_max_value is not None):
                pipseeker_cmd += [
                    "--snt-min-value",
                    f"{snt_min_value}",
                    "--snt-max-value",
                    f"{snt_max_value}",
                ]

            elif (snt_min_value is None) and (snt_max_value is None):
                pipseeker_cmd += [
                    "--snt-min-percent",
                    f"{snt_min_percent}",
                    "--snt-max-percent",
                    f"{snt_max_percent}",
                ]
            else:
                message(
                    typ="warning",
                    data={
                        "title": "PIPseeker parameters warning",
                        "body": "Scalars and percentile ranks for SNT feature plots cannot be used together in the same analysis",
                    },
                )

        if hto_fastq is not None:
            pipseeker_cmd += [
                "--hto-fastq",
                f"{hto_fastq.local_path}/.",  # Use period for directory input.
                "--hto-position",
                f"{hto_position}"
            ]

            if hto_tags is not None:
                pipseeker_cmd += ["--hto-tags", f"{hto_tags.local_path}"]

            if hto_annotation is not None:
                pipseeker_cmd += ["--hto-annotation", f"{hto_annotation.local_path}"]

            if hto_colormap is not None:
                pipseeker_cmd += ["--hto-colormap", f"{hto_colormap}"]

            if hto_colorbar is not None:
                pipseeker_cmd += ["--hto-colorbar", f"{hto_colorbar}"]

            if (hto_min_value is not None) and (hto_max_value is not None):
                pipseeker_cmd += [
                    "--hto-min-value",
                    f"{hto_min_value}",
                    "--hto-max-value",
                    f"{hto_max_value}"
                ]

            elif (hto_min_value is None) and (hto_max_value is None):
                pipseeker_cmd += [
                    "--hto-min-percent",
                    f"{hto_min_percent}",
                    "--hto-max-percent",
                    f"{hto_max_percent}",
                ]
            else:
                message(
                    typ="warning",
                    data={
                        "title": "PIPseeker parameters warning",
                        "body": "Scalars and percentile ranks for HTO feature plots "
                                "cannot be used together in the same analysis",
                    },
                )

    # PIPseeker buildmapref mode.
    elif pipseeker_mode == 'buildmapref_mode':
        custom_genome_reference_gtf_p = Path(custom_genome_reference_gtf)
        custom_genome_reference_fasta_p = Path(custom_genome_reference_fasta)

        # Define the local path for full and cells mode.
        local_output_dir = Path("/root/pipseeker_out")
        destination_directory = output_directory.remote_path

        pipseeker_cmd = [
            "pipseeker",
            "buildmapref",
            "--fasta",
            f"{custom_genome_reference_fasta_p}",
            "--gtf",
            f"{custom_genome_reference_gtf_p}",
            "--output-path",
            f"{local_output_dir}",
            "--read-length",
            f"{read_length}",
            "--sparsity",
            f"{sparsity}"
        ]

        pipseeker_cmd += universal_shared_args

        if include_types is not None and exclude_types is None:
            pipseeker_cmd += ["--include-types", f"{include_types}"]

            if biotype_tag is not None:
                pipseeker_cmd += ["--biotype-tag", f"{biotype_tag}"]

        elif exclude_types is not None and include_types is None:
            pipseeker_cmd += ["--exclude-types", f"{exclude_types}"]

            if biotype_tag is not None:
                pipseeker_cmd += ["--biotype-tag", f"{biotype_tag}"]

        elif exclude_types is not None and include_types is not None:
            message(
                typ="warning",
                data={
                    "title": "PIPseeker buildmapref parameters warning",
                    "body": "Only one of exclude_types and include_types can be used.",
                },
            )

        if additional_params_buildmapref is not None:
            additional_params_list = additional_params_buildmapref.split()
            pipseeker_cmd.extend(additional_params_list)

    #############################
    # Run PIPseeker
    #############################

    try:
        print(f'Running {" ".join(pipseeker_cmd)}')
        subprocess.run(pipseeker_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"PIPseeker failed with exit code {e.returncode}")
        # raise e
    finally:
        print(f"Uploading results from {local_output_dir} to {destination_directory}")
        return LatchOutputDir(str(local_output_dir), remote_path=destination_directory)

