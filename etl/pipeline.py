"""
etl/pipeline.py
Pipeline orchestrator — runs ingest → transform → validate in sequence.
"""

import logging
import time
from etl.ingest import ingest_all
from etl.transform import transform_all
from etl.validate import validate_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline():
    """Execute the full ETL pipeline."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("OPERATIONAL RISK DASHBOARD — ETL PIPELINE STARTING")
    logger.info("=" * 60)

    # Step 1: Ingest
    logger.info("\n📥 STEP 1: DATA INGESTION")
    total_rows = ingest_all()

    # Step 2: Transform
    logger.info("\n🔄 STEP 2: DATA TRANSFORMATION")
    transform_all()

    # Step 3: Validate
    logger.info("\n✅ STEP 3: DATA VALIDATION")
    quality_score = validate_all()

    elapsed = round(time.time() - start, 2)
    logger.info("\n" + "=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed}s")
    logger.info(f"  Rows Processed : {total_rows}")
    logger.info(f"  Quality Score  : {quality_score}%")
    logger.info("=" * 60)

    return {"rows_processed": total_rows, "quality_score": quality_score, "duration_seconds": elapsed}


if __name__ == "__main__":
    run_pipeline()
