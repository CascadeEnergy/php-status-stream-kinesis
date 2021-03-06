<?php

namespace CascadeEnergy\Tests\StatusStream\Kinesis;

use CascadeEnergy\StatusStream\Kinesis;

class KinesisTest extends \PHPUnit_Framework_TestCase
{
    /** @var Kinesis */
    private $kinesis;

    /** @var \PHPUnit_Framework_MockObject_MockObject */
    private $kinesisClient;

    public function setUp()
    {
        $this->kinesisClient = $this->getMockBuilder('Aws\Kinesis\KinesisClient')
            ->disableOriginalConstructor()
            ->setMethods(['putRecord'])
            ->getMock();

        /** @noinspection PhpParamsInspection */
        $this->kinesis = new Kinesis($this->kinesisClient, 'streamName');
        $this->kinesis->setSystemId('system', 'subsystem', 'component');
        $this->kinesis->setProcessId('foo:bar');
        $this->kinesis->setVersion('1.2.3');
    }

    public function testItShouldSendStatusUpdatesToKinesis()
    {
        $jsonEncodedData = json_encode([
            'system' => 'system',
            'subsystem' => 'subsystem',
            'component' => 'component',
            'processId' => 'foo:bar',
            'version' => '1.2.3',
            'state' => 'foo',
            'context' => ['bar' => 'baz']
        ]);

        $expectedParameters = [
            'StreamName' => 'streamName',
            'Data' => $jsonEncodedData,
            'PartitionKey' => 'foo:bar'
        ];

        $this->kinesisClient
            ->expects($this->once())
            ->method('putRecord')
            ->with($expectedParameters);

        $this->kinesis->update('foo', ['bar' => 'baz']);
    }

    /**
     * @dataProvider stateHelperFunctionDataProvider
     */
    public function testTheHelperFunctionsForEachStateShouldSendRecordsToKinesis($stateName)
    {
        $jsonEncodedData = json_encode([
            'system' => 'system',
            'subsystem' => 'subsystem',
            'component' => 'component',
            'processId' => 'foo:bar',
            'version' => '1.2.3',
            'state' => $stateName,
            'context' => ['qux' => 'quux']
        ]);

        $expectedParameters = [
            'StreamName' => 'streamName',
            'Data' => $jsonEncodedData,
            'PartitionKey' => 'foo:bar'
        ];

        $this->kinesisClient
            ->expects($this->once())
            ->method('putRecord')
            ->with($expectedParameters);

        $this->kinesis->$stateName(['qux'=>'quux']);
    }

    public function stateHelperFunctionDataProvider()
    {
        return [
            ['active'],
            ['idle'],
            ['degraded'],
            ['failed']
        ];
    }
}
