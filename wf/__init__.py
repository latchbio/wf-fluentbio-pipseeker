from typing import Optional

from latch import workflow
from latch.types import (
    LatchAuthor,
    LatchDir,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
    Params,
    Section,
    Fork,
    ForkBranch,
    Spoiler,
    Text,
    LatchAppearanceType,
)
from latch.resources.launch_plan import LaunchPlan

from wf.pipseeker import *

metadata = LatchMetadata(
    display_name="Fluent BioSciences PIPseeker v3.0.5",
    documentation="",
    author=LatchAuthor(
        name="LatchBio",
    ),
    repository="https://github.com/latchbio/wf-fluentbio-pipseeker",
    license="MIT",
    parameters={
        "fastq_directory": LatchParameter(
            display_name="FASTQ Directory",
            description="Directory of input FASTQ files. All FASTQ files in the directory will be used.",
            batch_table_column=True,
        ),
        "genome_source": LatchParameter(display_name="Genome Reference"),
        "compiled_genome_reference": LatchParameter(
            display_name="Compiled Genome",
            description="Reference genome to be used",
            batch_table_column=True,
        ),
        "custom_compiled_genome_zipped": LatchParameter(
            display_name="Zipped Compiled Custom Genome (.tar.gz or .zip)",
            placeholder=".tar.gz or .zip zipped file",
            description="Zipped (tar.gz) file of custom compiled PIPseeker genome",
            batch_table_column=True,
        ),
        "custom_compiled_genome": LatchParameter(
            display_name="Unzipped Compiled Custom Genome Directory",
            placeholder="Unzipped directory with custom compiled PIPseeker genome",
            description="Directory with custom compiled PIPseeker genome",
            batch_table_column=True,
        ),
        "custom_genome_reference_fasta": LatchParameter(
            display_name="Genome FASTA",
            description="Reference genome FASTA to be used",
            batch_table_column=True,
        ),
        "custom_genome_reference_gtf": LatchParameter(
            display_name="Genome GTF",
            description="Reference genome GTF to be used",
            batch_table_column=True,
        ),
        "include_types": LatchParameter(
            display_name="Include Types",
            description="Comma-separated list of biotypes to include or exclude from the reference. Only one of these arguments can be used. If neither is specified, all biotypes will be included.",
            batch_table_column=True,
        ),
        "exclude_types": LatchParameter(
            display_name="Exclude Types",
            description="Comma-separated list of biotypes to include or exclude from the reference. Only one of these arguments can be used. If neither is specified, all biotypes will be included.",
            batch_table_column=True,
        ),
        "biotype_tag": LatchParameter(
            display_name="Biotype Tag",
            description="Tag in the GTF file to use for determining biotype. Only use in conjunction with --include-biotypes or --exclude-biotypes when explicitly specifying biotypes.",
            batch_table_column=True,
        ),
        "read_length": LatchParameter(
            display_name="Read Length",
            description="Expected length of read 2. This is used to adjust STARs sjdbOverhang parameter.",
            batch_table_column=True,
        ),
        "sparsity": LatchParameter(
            display_name="Sparsity",
            description="Sparsity of suffix array. This is used to adjust STARs genomeSAsparseD parameter. Lower values will result in faster performance at the expense of bigger reference files and higher memory consumption.",
            batch_table_column=True,
        ),
        "additional_params_buildmapref": LatchParameter(
            display_name="Additional Parameters",
            description="Additional STAR command-line parameters in the form: --<name> <value>. Input the entire set of parameter names and values as a single string.",
            batch_table_column=True,
        ),
        "chemistry": LatchParameter(
            display_name="Chemistry",
            batch_table_column=True,
        ),
        "verbosity": LatchParameter(
            display_name="Verbosity",
            batch_table_column=True,
        ),
        "random_seed": LatchParameter(
            display_name="Random Seed",
            batch_table_column=True,
        ),
        "save_svg": LatchParameter(
            display_name="Save SVG",
            batch_table_column=True,
        ),
        "dpi": LatchParameter(
            display_name="DPI",
            batch_table_column=True,
        ),
        "downsample": LatchParameter(
            display_name="Downsample To",
            batch_table_column=True,
        ),
        "retain_barcoded_fastqs": LatchParameter(
            display_name="Retain Barcoded FASTQs",
            batch_table_column=True,
        ),
        "sorted_bam": LatchParameter(
            display_name="Generate Sorted BAM",
            batch_table_column=True,
        ),
        "remove_bam": LatchParameter(
            display_name="Remove BAM",
            batch_table_column=True,
        ),
        "exons_only": LatchParameter(
            display_name="Exons Only",
            batch_table_column=True,
        ),
        "min_sensitivity": LatchParameter(
            display_name="Minimum Sensitivity",
            batch_table_column=True,
        ),
        "max_sensitivity": LatchParameter(
            display_name="Maximum Sensitivity",
            batch_table_column=True,
        ),
        "force_cells": LatchParameter(
            display_name="Force Cells",
            batch_table_column=True,
        ),
        "run_barnyard": LatchParameter(
            display_name="Run Barnyard",
            batch_table_column=True,
        ),
        "clustering_percent_genes": LatchParameter(
            display_name="Clustering Percent Genes",
            batch_table_column=True,
        ),
        "diff_exp_genes": LatchParameter(
            display_name="Diff Exp Genes",
            batch_table_column=True,
        ),
        "principal_components": LatchParameter(
            display_name="Principal Components",
            batch_table_column=True,
        ),
        "nearest_neighbors": LatchParameter(
            display_name="Nearest Neighbours",
            batch_table_column=True,
        ),
        "resolution": LatchParameter(
            display_name="Resolution",
            batch_table_column=True,
        ),
        "clustering_sensitivity": LatchParameter(
            display_name="Clustering Sensitivity",
            batch_table_column=True,
        ),
        "min_clusters_kmeans": LatchParameter(
            display_name="Min Clusters kMeans",
            batch_table_column=True,
        ),
        "max_clusters_kmeans": LatchParameter(
            display_name="Max Clusters kMeans",
            batch_table_column=True,
        ),
        "umap_axes": LatchParameter(
            display_name="UMAP Axes",
            batch_table_column=True,
        ),
        "annotation": LatchParameter(
            display_name="Annotation",
            batch_table_column=True,
        ),
        "report_id": LatchParameter(
            display_name="Report ID",
            batch_table_column=True,
        ),
        "report_description": LatchParameter(
            display_name="Report Description",
            batch_table_column=True,
        ),
        "adt_fastq": LatchParameter(
            display_name="ADT FASTQ Path",
            batch_table_column=True,
        ),
        "adt_tags": LatchParameter(
            display_name="ADT Tags path",
            batch_table_column=True,
        ),
        "adt_position": LatchParameter(
            display_name="ADT Position",
            batch_table_column=True,
        ),
        "adt_annotation": LatchParameter(
            display_name="ADT Annotation",
            batch_table_column=True,
        ),
        "adt_colormap": LatchParameter(
            display_name="ADT Colormap",
            batch_table_column=True,
        ),
        "adt_min_percent": LatchParameter(
            display_name="ADT Min Percent",
            batch_table_column=True,
        ),
        "adt_max_percent": LatchParameter(
            display_name="ADT Max Percent",
            batch_table_column=True,
        ),
        "adt_min_value": LatchParameter(
            display_name="ADT Min Value",
            batch_table_column=True,
        ),
        "adt_max_value": LatchParameter(
            display_name="ADT Max Value",
            batch_table_column=True,
        ),
        "hto_fastq": LatchParameter(
            display_name="HTO FASTQ Path",
            batch_table_column=True,
        ),
        "hto_tags": LatchParameter(
            display_name="HTO Tags path",
            batch_table_column=True,
        ),
        "hto_position": LatchParameter(
            display_name="HTO Position",
            batch_table_column=True,
        ),
        "hto_annotation": LatchParameter(
            display_name="HTO Annotation",
            batch_table_column=True,
        ),
        "hto_colormap": LatchParameter(
            display_name="HTO Colormap",
            batch_table_column=True,
        ),
        "hto_min_percent": LatchParameter(
            display_name="HTO Min Percent",
            batch_table_column=True,
        ),
        "hto_max_percent": LatchParameter(
            display_name="HTO Max Percent",
            batch_table_column=True,
        ),
        "hto_min_value": LatchParameter(
            display_name="HTO Min Value",
            batch_table_column=True,
        ),
        "hto_max_value": LatchParameter(
            display_name="HTO Max Value",
            batch_table_column=True,
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            description="Output Directory",
            batch_table_column=True,
        ),
    },
    tags=[],
    flow=[
        Section(
            "Basic Inputs",
            Params("fastq_directory", "chemistry"),
            Fork(
                "genome_source",
                "",
                compiled=ForkBranch(
                    "Compiled Reference Genome",
                    Params(
                        "compiled_genome_reference",
                    ),
                ),
                custom_compiled=ForkBranch(
                    "Precompiled Custom Reference Genome",
                    Text("Choose one of the following:"),
                    Params(
                        "custom_compiled_genome_zipped",
                        "custom_compiled_genome",
                    ),
                ),
                custom_build=ForkBranch(
                    "Generate Custom Reference Genome",
                    Params(
                        "custom_genome_reference_fasta",
                        "custom_genome_reference_gtf",
                    ),
                    Spoiler(
                        "Additional Parameters",
                        Params(
                            "include_types",
                            "exclude_types",
                            "biotype_tag",
                            "read_length",
                            "sparsity",
                            "additional_params_buildmapref",
                        ),
                    ),
                ),
            ),
            Params("output_directory"),
        ),
        Spoiler(
            "Additional Parameters",
            Section(
                "General",
                Params(
                    "verbosity",
                    "random_seed",
                    "dpi",
                    "save_svg",
                ),
            ),
            Section(
                "FASTQ Processing",
                Params(
                    "downsample",
                    "retain_barcoded_fastqs",
                ),
            ),
            Section(
                "Mapping",
                Params(
                    "sorted_bam",
                    "remove_bam",
                ),
            ),
            Section(
                "Molecular Counting",
                Params(
                    "exons_only",
                ),
            ),
            Section(
                "Cell Calling",
                Params(
                    "min_sensitivity",
                    "max_sensitivity",
                    "force_cells",
                ),
            ),
            Section(
                "Barnyard Analysis",
                Params(
                    "run_barnyard",
                ),
            ),
            Section(
                "Clustering",
                Params(
                    "clustering_percent_genes",
                    "diff_exp_genes",
                    "principal_components",
                    "nearest_neighbors",
                    "resolution",
                    "clustering_sensitivity",
                    "min_clusters_kmeans",
                    "max_clusters_kmeans",
                    "umap_axes",
                ),
            ),
            Section(
                "Cell Type Annotation",
                Params(
                    "annotation",
                ),
            ),
            Section(
                "Report",
                Params(
                    "report_id",
                    "report_description",
                ),
            ),
            Section(
                "ADT",
                Params(
                    "adt_fastq",
                    "adt_tags",
                    "adt_position",
                    "adt_annotation",
                    "adt_colormap",
                    "adt_min_percent",
                    "adt_max_percent",
                    "adt_min_value",
                    "adt_max_value",
                ),
            ),
            Section(
                "HTO",
                Params(
                    "hto_fastq",
                    "hto_tags",
                    "hto_position",
                    "hto_annotation",
                    "hto_colormap",
                    "hto_min_percent",
                    "hto_max_percent",
                    "hto_min_value",
                    "hto_max_value",
                ),
            ),
        ),
    ],
)


@workflow(metadata)
def pipseeker_wf(
    fastq_directory: LatchDir,
    genome_source: str,
    compiled_genome_reference: GenomeType,
    custom_compiled_genome: Optional[LatchDir],
    custom_compiled_genome_zipped: Optional[LatchFile],
    custom_genome_reference_fasta: LatchFile,
    custom_genome_reference_gtf: LatchFile,
    include_types: Optional[str] = None,
    exclude_types: Optional[str] = None,
    biotype_tag: Optional[str] = None,
    read_length: Optional[int] = 100,
    sparsity: Optional[int] = 3,
    additional_params_buildmapref: Optional[str] = None,
    chemistry: Chemistry = Chemistry.v4,
    output_directory: LatchOutputDir = LatchOutputDir("latch:///PIPseeker_Output"),
    verbosity: Verbosity = Verbosity.two,
    random_seed: int = 0,
    save_svg: bool = False,
    dpi: int = 200,
    downsample: Optional[int] = None,
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
    adt_fastq: Optional[LatchFile] = None,
    adt_tags: Optional[LatchFile] = None,
    adt_position: int = 0,
    adt_annotation: Optional[LatchFile] = None,
    adt_colormap: str = "gray-to-green",
    adt_min_percent: int = 1,
    adt_max_percent: int = 99,
    adt_min_value: Optional[int] = None,
    adt_max_value: Optional[int] = None,
    hto_fastq: Optional[LatchFile] = None,
    hto_tags: Optional[LatchFile] = None,
    hto_position: int = 0,
    hto_annotation: Optional[LatchFile] = None,
    hto_colormap: str = "gray-to-red",
    hto_min_percent: int = 1,
    hto_max_percent: int = 99,
    hto_min_value: Optional[int] = None,
    hto_max_value: Optional[int] = None,
) -> LatchOutputDir:
    """Fluent BioSciences PIPseeker

    # Fluent BioSciences PIPseeker

    PIPseeker analyzes single-cell RNA data obtained with [Fluent BioSciences](https://www.fluentbio.com/products/pipseeker-software-for-data-analysis/) proprietary PIPseq™ 3 Single Cell RNA (scRNA-seq) Kits.

    PIPseeker offers a comprehensive analysis solution that provides the user with detailed metrics, gene expression profiles, basic cell quality and clustering indicators, and cell type annotation for some sample types.
    The outputs of PIPseeker can then be used for subsequent, specialized tertiary analysis streams.
    PIPseeker also supports specialized applications like measuring surface protein levels using antibody-derived tags (ADTs) and cell hashing using hashtag oligonucleotides (HTOs).

    """

    return pipseeker_task(
        fastq_directory=fastq_directory,
        chemistry=chemistry,
        genome_source=genome_source,
        compiled_genome_reference=compiled_genome_reference,
        custom_compiled_genome=custom_compiled_genome,
        custom_compiled_genome_zipped=custom_compiled_genome_zipped,
        custom_genome_reference_fasta=custom_genome_reference_fasta,
        custom_genome_reference_gtf=custom_genome_reference_gtf,
        include_types=include_types,
        exclude_types=exclude_types,
        biotype_tag=biotype_tag,
        read_length=read_length,
        sparsity=sparsity,
        additional_params_buildmapref=additional_params_buildmapref,
        output_directory=output_directory,
        sorted_bam=sorted_bam,
        verbosity=verbosity,
        random_seed=random_seed,
        save_svg=save_svg,
        dpi=dpi,
        remove_bam=remove_bam,
        downsample=downsample,
        retain_barcoded_fastqs=retain_barcoded_fastqs,
        exons_only=exons_only,
        min_sensitivity=min_sensitivity,
        max_sensitivity=max_sensitivity,
        force_cells=force_cells,
        run_barnyard=run_barnyard,
        clustering_percent_genes=clustering_percent_genes,
        diff_exp_genes=diff_exp_genes,
        principal_components=principal_components,
        nearest_neighbors=nearest_neighbors,
        resolution=resolution,
        clustering_sensitivity=clustering_sensitivity,
        min_clusters_kmeans=min_clusters_kmeans,
        max_clusters_kmeans=max_clusters_kmeans,
        umap_axes=umap_axes,
        annotation=annotation,
        report_id=report_id,
        report_description=report_description,
        adt_fastq=adt_fastq,
        adt_tags=adt_tags,
        adt_position=adt_position,
        adt_annotation=adt_annotation,
        adt_colormap=adt_colormap,
        adt_min_percent=adt_min_percent,
        adt_max_percent=adt_max_percent,
        adt_min_value=adt_min_value,
        adt_max_value=adt_max_value,
        hto_fastq=hto_fastq,
        hto_tags=hto_tags,
        hto_position=hto_position,
        hto_annotation=hto_annotation,
        hto_colormap=hto_colormap,
        hto_min_percent=hto_min_percent,
        hto_max_percent=hto_max_percent,
        hto_min_value=hto_min_value,
        hto_max_value=hto_max_value,
    )


LaunchPlan(
    pipseeker_wf,
    "PIPseeker Sample1",
    {
        "fastq_directory": LatchDir("s3://latch-public/test-data/18440/sample1"),
        "chemistry": Chemistry.v4,
        "output_directory": LatchOutputDir("latch:///PIPseeker_Output/Sample1"),
    },
)

LaunchPlan(
    pipseeker_wf,
    "PIPseeker Sample2",
    {
        "fastq_directory": LatchDir("s3://latch-public/test-data/18440/sample2"),
        "chemistry": Chemistry.v4,
        "output_directory": LatchOutputDir("latch:///PIPseeker_Output/Sample2"),
    },
)
