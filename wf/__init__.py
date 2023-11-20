from typing import Optional

from latch import workflow
from latch.types import (
    LatchAuthor,
    LatchDir,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
)
from latch.resources.launch_plan import LaunchPlan
from wf.pipseeker import Chemistry, pipseeker_task

metadata = LatchMetadata(
    display_name="Fluent BioSciences PIPseeker",
    documentation="",
    author=LatchAuthor(
        name="LatchBio",
    ),
    repository="github.com/latchbio/pipseeker",
    license="MIT",
    parameters={
        "fastqs": LatchParameter(
            display_name="FastQ Directory",
            description="Directory of input FASTQ files.",
            batch_table_column=True,
        ),
        "chemistry": LatchParameter(
            display_name="Chemistry",
            batch_table_column=True,
        ),
        # "verbosity": LatchParameter(
        #     display_name="Verbosity",
        #     batch_table_column=True,
        # ),
        # "run_barnyard": LatchParameter(
        #     display_name="Run Barnyard",
        #     batch_table_column=True,
        # ),
        "sorted_bam": LatchParameter(
            display_name="Generate Sorted BAM",
            batch_table_column=True,
        ),
        # "remove_bam": LatchParameter(
        #     display_name="Remove BAM",
        #     batch_table_column=True,
        # ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            batch_table_column=True,
        ),
    },
    tags=[],
)


@workflow(metadata)
def pipseeker_wf(
    fastqs: LatchDir,
    output_directory: LatchOutputDir,
    chemistry: Chemistry = Chemistry.v4,
    sorted_bam: bool = False,
    # verbosity: Optional[Verbosity] = None,
    # run_barnyard: bool = False,
    # remove_bam: bool = False,
) -> LatchOutputDir:
    """Fluent BioSciences PIPseeker

    # Fluent BioSciences PIPseeker

    PIPseeker analyzes single-cell RNA data obtained with [Fluent BioSciences](https://www.fluentbio.com/) proprietary PIPseq™ 3 Single Cell RNA (scRNA-seq) Kits.

    PIPseeker offers a comprehensive analysis solution that provides the user with detailed metrics, gene expression profiles, basic cell quality and clustering indicators, and cell type annotation for some sample types.
    The outputs of PIPseeker can then be used for subsequent, specialized tertiary analysis streams.
    PIPseeker also supports specialized applications like measuring surface protein levels using antibody-derived tags (ADTs) and cell hashing using hashtag oligonucleotides (HTOs).

    This version of PIPseeker is an MVP built by LatchBio. Currently, it includes the default `pipseeker-gex-reference-GRCh38-2022.04.tar.gz` reference for genomic analysis. Additional reference genomes and parameters can be made available upon request to cater to specific research needs and applications.
    """

    return pipseeker_task(
        fastqs=fastqs,
        chemistry=chemistry,
        sorted_bam=sorted_bam,
        output_directory=output_directory,
        # verbosity=verbosity,
        # run_barnyard=run_barnyard,
        # remove_bam=remove_bam,
    )


LaunchPlan(
    pipseeker_wf,
    "PIPseeker Sample1",
    {
        "fastqs": LatchDir("s3://latch-public/test-data/18440/sample1"),
        "chemistry": Chemistry.v4,
        "sorted_bam": True,
        "output_directory": LatchOutputDir("latch:///PIPseeker_Output/Sample1")
        # "verbosity": Verbosity.two,
    },
)

LaunchPlan(
    pipseeker_wf,
    "PIPseeker Sample2",
    {
        "fastqs": LatchDir("s3://latch-public/test-data/18440/sample2"),
        "chemistry": Chemistry.v4,
        "sorted_bam": True,
        "output_directory": LatchOutputDir("latch:///PIPseeker_Output/Sample2")
        # "verbosity": Verbosity.two,
    },
)
