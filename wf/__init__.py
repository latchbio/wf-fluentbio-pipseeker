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

shared_parameters = {
    "pipseeker_mode": LatchParameter(
        display_name='PIPseeker Mode',
        description="full mode (standard runs), "
                    "cells mode (rerunning cell calling from existing output), "
                    "or buildmapref (building a custom mapping reference)")
}

full_mode_parameters = {
    "fastq_directory": LatchParameter(
        display_name="FASTQ Directory",
        description="Directory of input FASTQ files. All FASTQ files in the directory will be used.",
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
    "downsample_to": LatchParameter(
        display_name="Downsample",
        batch_table_column=True,
    ),
    "input_reads": LatchParameter(
        display_name="Number of Input Reads",
        description="The total number of reads in the provided fastq, needed only when downsampling. "
                    "If not provided, reads will be counted manually."
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
    "snt_fastq": LatchParameter(
        display_name="SNT FASTQ Path",
        batch_table_column=True,
    ),
    "snt_tags": LatchParameter(
        display_name="SNT Tags path",
        batch_table_column=True,
    ),
    "snt_position": LatchParameter(
        display_name="SNT Position",
        batch_table_column=True,
    ),
    "snt_annotation": LatchParameter(
        display_name="SNT Annotation",
        batch_table_column=True,
    ),
    "snt_colormap": LatchParameter(
        display_name="SNT Colormap",
        batch_table_column=True,
    ),
    "snt_min_percent": LatchParameter(
        display_name="SNT Min Percent",
        batch_table_column=True,
    ),
    "snt_max_percent": LatchParameter(
        display_name="SNT Max Percent",
        batch_table_column=True,
    ),
    "snt_min_value": LatchParameter(
        display_name="SNT Min Value",
        batch_table_column=True,
    ),
    "snt_max_value": LatchParameter(
        display_name="SNT Max Value",
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
    "hto_colorbar": LatchParameter(
        display_name="HTO Colorbar",
        batch_table_column=True,
    ),
    "output_directory": LatchParameter(
        display_name="Output Directory",
        description="Output Directory",
        batch_table_column=True,
    )}

cells_mode_parameters = {
    "previous": LatchParameter(display_name="Previous Directory",
                               description="Path to results from a previous PIPseeker run",
                               batch_table_column=True),
    "hash_cellsmode": LatchParameter(display_name="Hash",
                                     description="Run the analysis for a single demultiplexed sample with this hash tag.",
                                     batch_table_column=True)
}

buildmapref_mode_parameters = {
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
        description="Comma-separated list of biotypes to include or exclude from the reference. "
                    "Only one of these arguments can be used. If neither is specified, all biotypes will be included.",
        batch_table_column=True,
    ),
    "exclude_types": LatchParameter(
        display_name="Exclude Types",
        description="Comma-separated list of biotypes to include or exclude from the reference. "
                    "Only one of these arguments can be used. If neither is specified, all biotypes will be included.",
        batch_table_column=True,
    ),
    "biotype_tag": LatchParameter(
        display_name="Biotype Tag",
        description="Tag in the GTF file to use for determining biotype. "
                    "Only use in conjunction with --include-biotypes or --exclude-biotypes "
                    "when explicitly specifying biotypes.",
        batch_table_column=True,
    ),
    "read_length": LatchParameter(
        display_name="Read Length",
        description="Expected length of read 2. This is used to adjust STARs sjdbOverhang parameter.",
        batch_table_column=True,
    ),
    "sparsity": LatchParameter(
        display_name="Sparsity",
        description="Sparsity of suffix array. This is used to adjust STARs genomeSAsparseD parameter. "
                    "Lower values will result in faster performance at the expense of "
                    "bigger reference files and higher memory consumption.",
        batch_table_column=True,
    ),
    "additional_params_buildmapref": LatchParameter(
        display_name="Additional STAR Parameters",
        description="Additional STAR command-line parameters in the form: --<name> <value>. "
                    "Input the entire set of parameter names and values as a single string.",
        batch_table_column=True,
    )
}

#################
#  GUI Buildout

shared_full_cells_mode_spoiler_section = Section("",
                                                 Section(
                                                     "Cell Calling",
                                                     Params(
                                                         "min_sensitivity",
                                                         "max_sensitivity",
                                                         "force_cells",
                                                     ),
                                                 ),
                                                 Section(
                                                     "Cell Type Annotation",
                                                     Params(
                                                         "annotation",
                                                     ),
                                                 ),
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
                                                     "Report",
                                                     Params(
                                                         "report_id",
                                                         "report_description",
                                                     ),
                                                 ),
                                                 Section(
                                                     "SNT",
                                                     Params(
                                                         "snt_fastq",
                                                         "snt_tags",
                                                         "snt_position",
                                                         "snt_annotation",
                                                         "snt_colormap",
                                                         "snt_min_percent",
                                                         "snt_max_percent",
                                                         "snt_min_value",
                                                         "snt_max_value",
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
                                                         "hto_colorbar",
                                                     ),
                                                 )
                                                 )


full_mode_spoiler_section = Section("",
                                    Section(
                                        "FASTQ Processing",
                                        Params(
                                            "downsample_to",
                                            "input_reads",
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
                                        "Barnyard Analysis",
                                        Params(
                                            "run_barnyard",
                                        ),
                                    ),
                                    )


full_mode_mapping_section = Section(
    "Mapping Reference",
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
            )
        )
    )
)


section_full_mode = Section(
    "Full Mode",
    Params("output_directory",
           "fastq_directory",
           "chemistry"
    ),
    full_mode_mapping_section,
    Spoiler(
        "Additional Parameters",
        full_mode_spoiler_section,
        shared_full_cells_mode_spoiler_section)
)


section_cells_mode = Section(
    "Cells Mode",
    Params("output_directory",
           "previous",
    ),
    Spoiler(
        "Additional Parameters",
        shared_full_cells_mode_spoiler_section,
        Params("hash_cellsmode")
    )
)


section_buildmapref = Section(
    "Generate Custom Reference Genome",
    Params(
        "output_directory",
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
        )
    )
)


metadata = LatchMetadata(
    display_name="Fluent BioSciences PIPseeker v3.1.3",
    documentation="",
    author=LatchAuthor(
        name="Fluent BioSciences"),
    repository="https://github.com/latchbio/wf-fluentbio-pipseeker",
    license="https://www.fluentbio.com/wp-content/uploads/2022/06/EULA.pdf",
    parameters={**shared_parameters,
                **full_mode_parameters,
                **cells_mode_parameters,
                **buildmapref_mode_parameters},
    tags=[],
    flow=[
        Section("PIPseeker Configuration",
                Fork(fork="pipseeker_mode",
                     display_name="",
                     full_mode=ForkBranch('Full Mode',
                                          section_full_mode),
                     cells_mode=ForkBranch('Cells',
                                           section_cells_mode),
                     buildmap_mode=ForkBranch('Mapping Ref',
                                              section_buildmapref)
                     )
                )
    ],
)


@workflow(metadata)
def pipseeker_wf(*,
                 pipseeker_mode: str,
                 output_directory: LatchOutputDir = LatchOutputDir("latch:///PIPseeker_Output"),
                 fastq_directory: LatchDir,
                 chemistry: Chemistry = Chemistry.v4,
                 genome_source: str,
                 compiled_genome_reference: GenomeType,
                 custom_compiled_genome: Optional[LatchDir],
                 custom_compiled_genome_zipped: Optional[LatchFile],
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
                 previous: LatchDir,
                 hash_cellsmode: Optional[str] = None,

                 # buildmapref mode args
                 custom_genome_reference_fasta: LatchFile,
                 custom_genome_reference_gtf: LatchFile,
                 include_types: Optional[str] = None,
                 exclude_types: Optional[str] = None,
                 biotype_tag: Optional[str] = None,
                 read_length: Optional[int] = 100,
                 sparsity: Optional[int] = 3,
                 additional_params_buildmapref: Optional[str] = None
                 ) -> LatchOutputDir:

    return pipseeker_task(
        pipseeker_mode=pipseeker_mode,
        output_directory=output_directory,
        fastq_directory=fastq_directory,
        chemistry=chemistry,
        genome_source=genome_source,
        compiled_genome_reference=compiled_genome_reference,
        custom_compiled_genome=custom_compiled_genome,
        custom_compiled_genome_zipped=custom_compiled_genome_zipped,
        sorted_bam=sorted_bam,
        verbosity=verbosity,
        random_seed=random_seed,
        save_svg=save_svg,
        dpi=dpi,
        remove_bam=remove_bam,
        downsample_to=downsample_to,
        input_reads=input_reads,
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
        snt_fastq=snt_fastq,
        snt_tags=snt_tags,
        snt_position=snt_position,
        snt_annotation=snt_annotation,
        snt_colormap=snt_colormap,
        snt_min_percent=snt_min_percent,
        snt_max_percent=snt_max_percent,
        snt_min_value=snt_min_value,
        snt_max_value=snt_max_value,
        hto_fastq=hto_fastq,
        hto_tags=hto_tags,
        hto_position=hto_position,
        hto_annotation=hto_annotation,
        hto_colormap=hto_colormap,
        hto_colorbar=hto_colorbar,
        hto_min_percent=hto_min_percent,
        hto_max_percent=hto_max_percent,
        hto_min_value=hto_min_value,
        hto_max_value=hto_max_value,

        # cells mode args
        hash_cellsmode=hash_cellsmode,
        previous=previous,

        # buildmapref mode args
        custom_genome_reference_fasta=custom_genome_reference_fasta,
        custom_genome_reference_gtf=custom_genome_reference_gtf,
        include_types=include_types,
        exclude_types=exclude_types,
        biotype_tag=biotype_tag,
        read_length=read_length,
        sparsity=sparsity,
        additional_params_buildmapref=additional_params_buildmapref
    )


# Testing

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
