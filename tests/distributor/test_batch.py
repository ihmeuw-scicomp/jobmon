from jobmon.distributor.batch import Batch
from jobmon.server.web.models.api import Batch as ServerBatch


def test_batch_resource_initialization(distributor_crud, requester_no_retry):

    database_ids = distributor_crud()

    # Create a single batch, test its relationships
    batch_id = database_ids['batch_ids'][0]
    task_resources_id = database_ids['task_resources_ids'][0]
    array_id = database_ids['array_ids'][0]

    batch = Batch(
        batch_id=batch_id, task_resources_id=task_resources_id,
        array_id=array_id, array_name='foo', requester=requester_no_retry
    )

    # Test that we can initialize the batch
    batch.load_requested_resources()

    assert batch._requested_resources['queue'] == 'null.q'
    assert batch._requested_resources['foo'] == 'bar'
