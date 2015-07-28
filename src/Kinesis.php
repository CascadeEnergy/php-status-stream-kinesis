<?php

namespace CascadeEnergy\StatusStream;

use Aws\Kinesis\KinesisClient;

class Kinesis implements StatusStreamInterface
{
    private $component = '';
    private $kinesis;
    private $machineId;
    private $streamName;
    private $system = '';
    private $subsystem = '';

    public function __construct(KinesisClient $kinesis, $streamName)
    {
        $this->kinesis = $kinesis;
        $this->streamName = $streamName;
    }

    /**
     * @param string $system
     * @param string $subsystem
     * @param string $component
     */
    public function setSystemId($system, $subsystem = '', $component = '')
    {
        $this->system = $system;
        $this->subsystem = $subsystem;
        $this->component = $component;
    }

    /**
     * @param array $machineId
     */
    public function setMachineId(array $machineId)
    {
        $this->machineId = implode(':', $machineId);
    }

    /**
     * Updates the current state to `active`, possibly with some context data
     *
     * @param mixed|null $context
     */
    public function active($context = null)
    {
        $this->update('active', $context);
    }

    /**
     * Updates the current state to `degraded`, possibly with some context data
     *
     * @param mixed|null $context
     */
    public function degraded($context = null)
    {
        $this->update('degraded', $context);
    }

    /**
     * Updates the current state to `failed`, possibly with some context data
     *
     * @param mixed|null $context
     */
    public function failed($context = null)
    {
        $this->update('failed', $context);
    }

    /**
     * Updates the current state to `idle`, possibly with some context data
     *
     * @param mixed|null $context
     */
    public function idle($context = null)
    {
        $this->update('idle', $context);
    }

    /**
     * Updates the current state to given state, possibly with some context data
     *
     * @param string $state
     * @param mixed|null $context
     */
    public function update($state, $context = null)
    {
        $data = [
            'system' => $this->system,
            'subsystem' => $this->subsystem,
            'component' => $this->component,
            'machineId' => $this->machineId,
            'state' => $state,
            'context' => $context
        ];

        $this->kinesis->putRecord([
            'StreamName' => $this->streamName,
            'Data' => json_encode($data),
            'PartitionKey' => $this->machineId
        ]);
    }
}
