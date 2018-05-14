<?php

namespace App\Console\Commands;

error_reporting(E_ALL ^ E_DEPRECATED);
ini_set('max_execution_time', 0);
use App\HackRecord;
use App\HackSource;
use App\Jobs\BatchInsertToMysql;
use App\MissingHackSourceRecord;
use Illuminate\Console\Command;
use Cassandra;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;

class CassandraToMysqlImport extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'cassandra-sql:import';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Export from cassandra and import to mysql.';


    protected $hackRecord;
    protected $hackSource;
    protected $cluster;
    protected $session;
    protected $cassandraURL;
    protected $cassandraUsername;
    protected $cassandraPassword;
    protected $cassandraPort;
    protected $keySpace;
    protected $cassandraTimeout;
    protected $pageSize;
    protected $hackSources;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();

        $this->cassandraURL = explode(",",env('CASSANDRA_URL','127.0.0.1'));
        $this->cassandraUsername = env('CASSANDRA_USERNAME','');
        $this->cassandraPassword = env('CASSANDRA_PASSWORD','');
        $this->cassandraPort = (int)env('CASSANDRA_PORT',9042);
        $this->keySpace = env('CASSANDRA_KEYSPACE','hdb_new');;
        $this->cassandraTimeout = 600;
        $this->hackRecord = "hack_records";
        $this->pageSize = (int)env('CASSANDRA_PAGE_SIZE',10000);
        $this->cassandraConnection();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        try{
            Log::info("Process started.");
            if(Cache::get('hack_sources') == null){
                $this->getAllHackSources();
            }
            $this->hackSources = Cache::get('hack_sources');
            $index = 1;
            $sqlObject = array();
            $query = "SELECT * FROM $this->hackRecord";
            $statement = new Cassandra\SimpleStatement($query);
            $options = array('page_size' => $this->pageSize,'timeout' => $this->cassandraTimeout,'consistency' => Cassandra::CONSISTENCY_LOCAL_QUORUM);
            $result = $this->session->execute($statement,new Cassandra\ExecutionOptions($options));
            //$result = $this->session->execute($statement,['arguments' => $options]);
            while (true) {
                //$batch = new Cassandra\BatchStatement();
                foreach ($result as $row) {
                    if($index % 1000000 == 0) {
                        Mail::raw("Total record inserted:: $index", function ($message){
                            $message->to(env("USER_EMAIL"));
                            $message->subject("DB:import Record Count ");
                        });
                        Log::info("Total record inserted:: $index");
                    }
                    //Log::info("Sourceid::". $row['sourceid']->uuid()."::recordid::".$row['recordid']->uuid()."::email::".$row['email']);
                    if(array_key_exists($row['sourceid']->uuid(), $this->hackSources)) {
                        $sourceValue = $this->hackSources[$row['sourceid']->uuid()];
                        $data = [
                            'email' => checkIsEmpty($row['email']),
                            'sourceid' => $row['sourceid']->uuid(),
                            'recordid' => $row['recordid']->uuid(),
                            'attributes' => checkIsEmptyJson($row['attributes']),
                            'firstname' => checkIsEmpty($row['firstname']),
                            'ipaddress' => checkIsEmpty($row['ipaddress']),
                            'isdataclean' => checkIsEmptyConvertBoolean($row['isdataclean']),
                            'isremoved' => checkIsEmptyConvertBoolean($row['isremoved']),
                            'lastname' => checkIsEmpty($row['lastname']),
                            'password' => checkIsEmptyPassword($row['password']),
                            'passwordhash' => checkIsEmpty($row['passwordhash']),
                            'username' => checkIsEmptyUsername($row['username']),
                            'status' => checkIsEmpty($row['status']),
                            'dateinserted' => date('Y-m-d H:i:s'),
                            'emaildomain' => checkIsEmpty($row['emaildomain']),
                            'phonenumber' => checkIsEmptyAndRetrievePhone($row['attributes']),
                            'hack_source_id' => $sourceValue
                        ];
                        $sqlObject[] = $data;
                        $index = $index + 1;
                    }else{
                        $sourceValue = null;
                        MissingHackSourceRecord::create(
                            [
                                'email' => checkIsEmpty($row['email']),
                                'sourceid' => $row['sourceid']->uuid(),
                                'recordid' => $row['recordid']->uuid(),
                                'attributes' => checkIsEmptyJson($row['attributes']),
                                'firstname' => checkIsEmpty($row['firstname']),
                                'ipaddress' => checkIsEmpty($row['ipaddress']),
                                'isdataclean' => checkIsEmptyConvertBoolean($row['isdataclean']),
                                'isremoved' => checkIsEmptyConvertBoolean($row['isremoved']),
                                'lastname' => checkIsEmpty($row['lastname']),
                                'password' => checkIsEmptyPassword($row['password']),
                                'passwordhash' => checkIsEmpty($row['passwordhash']),
                                'username' => checkIsEmptyUsername($row['username']),
                                'status' => checkIsEmpty($row['status']),
                                'dateinserted' => date('Y-m-d H:i:s'),
                                'emaildomain' => checkIsEmpty($row['emaildomain']),
                                'phonenumber' => checkIsEmptyAndRetrievePhone($row['attributes'])
                            ]
                        );
                    }
                }

                $job = (new BatchInsertToMysql($sqlObject))->onQueue('high');
                dispatch($job);
                /*foreach (array_chunk($sqlObject, 3000) as $sqlData) {
                    HackRecord::insert($sqlData);
                }*/
                $sqlObject = array();
                //$updateResult = $this->sessionInsert->executeAsync($batch);
                if ($result->isLastPage()) { break; }
                $result = $result->nextPage();
            }
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("Queue:DB:import Error");
            });
        }
    }

    protected function cassandraConnection()
    {
        $this->cluster   = Cassandra::cluster($this->cassandraURL)                 // connects to localhost by default
        ->withPersistentSessions(true)
            //->withProtocolVersion(1)
            ->withTCPKeepalive(10)
            ->withDefaultTimeout($this->cassandraTimeout)
            ->withContactPoints(env('CASSANDRA_CONTACT_POINT'))
            ->withCredentials($this->cassandraUsername, $this->cassandraPassword)
            ->withPort($this->cassandraPort)
            ->build();
        $this->session = $this->cluster->connect($this->keySpace);
    }

    public function getAllHackSources()
    {
        $hackSources = HackSource::all();
        foreach($hackSources as $hackSource){
            $data[$hackSource->sourceid] = $hackSource->id;
        }
        Cache::forever('hack_sources', $data);
    }
}


