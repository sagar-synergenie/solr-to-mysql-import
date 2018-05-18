<?php

namespace App\Console\Commands;


error_reporting(E_ALL ^ E_DEPRECATED);
ini_set('max_execution_time', 0);
ini_set('memory_limit', '10024M');

use App\Jobs\CassandraToSqlCsvImportJob;
use Illuminate\Console\Command;
use Cassandra;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;
use App\MissingHackSourceRecord;
use App\HackRecord;
use App\HackSource;

class CassandraToSqlCsvImport extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'cassandra-csv:import';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Export from cassandra and import to mysql using CSV.';

    /**
     * Create a new command instance.
     *
     * @return void
     */

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
    public $fileName;

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
            $fileNameIndex = 1;
            Log::info("Process started.");
            $this->fileName = $this->fileName($fileNameIndex);
            $csv = storage_path("csv/$this->fileName");
            /*$query = sprintf("LOAD DATA local INFILE '%s'
                                IGNORE
                                INTO TABLE hack_records
                                FIELDS TERMINATED BY ','
                                OPTIONALLY ENCLOSED BY '\"'
                                ESCAPED BY '\"'
                                LINES TERMINATED BY '\\n'
                                IGNORE 1 ROWS
                                (@email,@attributes,@sourceid,@recordid,@firstname,@ipaddress,@isdataclean,@isremoved,@lastname,@password,@passwordhash,@username,@status,@dateinserted,@emaildomain,@phonenumber,@hack_source_id)
                                SET
                                email = nullif(@email,''),
                                sourceid = nullif(@sourceid,''),
                                recordid = nullif(@recordid,''),
                                attributes = nullif(@attributes,''),
                                firstname = nullif(@firstname,''),
                                ipaddress = nullif(@ipaddress,''),
                                isdataclean = nullif(@isdataclean,''),
                                isremoved = nullif(@isremoved,''),
                                lastname = nullif(@lastname,''),
                                password = nullif(@password,''),
                                passwordhash = nullif(@passwordhash,''),
                                username = nullif(@username,''),
                                status = nullif(@status,''),
                                dateinserted = nullif(@dateinserted,''),
                                emaildomain = nullif(@emaildomain,''),
                                phonenumber = nullif(@phonenumber,''),
                                hack_source_id = nullif(@hack_source_id,'')",addslashes($csv));*/


            /*$query = sprintf("LOAD DATA LOCALinfile '$csv'
                                INTO TABLE hack_records
                                fields terminated BY \",\"
                                lines terminated BY \"\n\"
                                IGNORE 1 LINES
                                (@attributes)
                                SET
                                attributes = nullif(@attributes,'')");*/
            //return DB::connection()->getpdo()->exec($query);

            if(Cache::get('hack_sources') == null){
                $this->getAllHackSources();
            }
            $output = fopen($csv,"w") or die("Can't open php://output");
            fputcsv($output, array('hack_source_id','email','emaildomain','username','firstname','lastname','password','passwordhash','ipaddress','phonenumber','attributes','status','isdataclean','isremoved','dateinserted'),";");
            $this->hackSources = Cache::get('hack_sources');
            $index = 1;
            $sqlObject = array();
            $query = "SELECT * FROM $this->hackRecord";
            $statement = new Cassandra\SimpleStatement($query);
            $options = array('page_size' => $this->pageSize,'timeout' => $this->cassandraTimeout,'consistency' => Cassandra::CONSISTENCY_LOCAL_QUORUM);
            $result = $this->session->execute($statement,new Cassandra\ExecutionOptions($options));
            //$result = $this->session->execute($statement,['arguments' => $options]);
            while (true) {
                foreach ($result as $row) {
                    if($index % 1000000 == 0) {
                        fclose($output);
                        $job = (new CassandraToSqlCsvImportJob($this->fileName,$index))->onQueue('high');
                        dispatch($job);
                        $fileNameIndex += 1;
                        $this->fileName = $this->fileName($fileNameIndex);
                        $csv = storage_path("csv/$this->fileName");
                        $output = fopen($csv,"w") or die("Can't open php://output");
                        fputcsv($output, array('hack_source_id','email','emaildomain','username','firstname','lastname','password','passwordhash','ipaddress','phonenumber','attributes','status','isdataclean','isremoved','dateinserted'),";");
                    }
                    //Log::info("Sourceid::". $row['sourceid']->uuid()."::recordid::".$row['recordid']->uuid()."::email::".$row['email']);
                    if(array_key_exists($row['sourceid']->uuid(), $this->hackSources)) {
                        $sourceValue = $this->hackSources[$row['sourceid']->uuid()];
                        $data = [
                            $sourceValue,
                            checkIsEmpty($row['email']),
                            checkIsEmpty($row['emaildomain']),
                            checkIsEmptyUsername($row['username']),
                            checkIsEmpty($row['lastname']),
                            checkIsEmpty($row['firstname']),
                            checkIsEmptyPassword($row['password']),
                            checkIsEmpty($row['passwordhash']),
                            checkIsEmpty($row['ipaddress']),
                            checkIsEmptyAndRetrievePhone($row['attributes']),
                            checkIsEmptyJson($row['attributes']),
                            checkIsEmpty($row['status']),
                            checkIsEmptyConvertBoolean($row['isdataclean']),
                            checkIsEmptyConvertBoolean($row['isremoved']),
                            strtotime(date('Y-m-d H:i:s')),
                        ];
                        fputcsv($output, $data,";");
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
                $sqlObject = array();
                if ($result->isLastPage()) { break; }
                $result = $result->nextPage();
            }
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Error");
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

    public function fileName($index)
    {
        return $index."_".now()->timestamp;
    }
}
