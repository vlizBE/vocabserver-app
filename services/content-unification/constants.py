import os

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
DATA_GRAPH = "http://mu.semte.ch/graphs/public"
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"

JOB_URI_PREFIX = "http://redpencil.data.gift/id/job/"
TASK_URI_PREFIX = "http://redpencil.data.gift/id/task/"
CONTAINER_URI_PREFIX = "http://redpencil.data.gift/id/container/"
FILTER_COUNT_INPUT_URI_PREFIX = "http://my-application.com/filter-count-input/"
FILTER_COUNT_OUTPUT_URI_PREFIX = "http://my-application.com/filter-count-output/"

FILE_RESOURCE_BASE = "http://example-resource.com/"

VOCAB_DELETE_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDeleteJob"
VOCAB_DELETE_WAIT_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDeleteWaitJob"
CONT_UN_OPERATION = "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob"
FILTER_COUNT_OPERATION =  "http://mu.semte.ch/vocabularies/ext/FilterCountJob"

STATUS_BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy"
STATUS_SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
STATUS_SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success"
STATUS_FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed"

RELATIVE_STORAGE_PATH = os.environ.get("MU_APPLICATION_FILE_STORAGE_PATH", "").rstrip("/")
STORAGE_PATH = f"/share/{RELATIVE_STORAGE_PATH}"

UNIFICATION_BATCH_SIZE = 10

