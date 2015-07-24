<?php

namespace Cascade\StatusStream;

use Aws\Kinesis\KinesisClient;

class KinesisStatusStream implements StatusStreamInterface
{
    private $kinesis;
    private $machineId;
    private $streamName;

    public function __construct(KinesisClient $kinesis, $streamName)
    {
        $this->kinesis = $kinesis;
        $this->streamName = $streamName;
    }

    public function updateStatus($state, $activity = '')
    {
        $data = [
            'system' => 'sensei3',
            'subsystem' => 'digest',
            'component' => 'ingest',
            'machineId' => $this->machineId,
            'state' => $state,
            'activity' => $activity
        ];

        $this->kinesis->putRecord([
            'StreamName' => $this->streamName,
            'Data' => json_encode($data),
            'PartitionKey' => $this->machineId
        ]);
    }

    public function active($activity = '')
    {
        $this->updateStatus('active', $activity);
    }

    public function idle($activity = '')
    {
        $this->updateStatus('idle', $activity);
    }

    public function warning($activity = '')
    {
        $this->updateStatus('warning', $activity);
    }

    /**
     * @param array $machineId
     */
    public function setMachineId(array $machineId)
    {
        $this->machineId = implode(':', $machineId);
    }
}
