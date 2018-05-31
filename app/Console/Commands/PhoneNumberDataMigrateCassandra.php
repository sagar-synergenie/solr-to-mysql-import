<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Cassandra;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;

class PhoneNumberDataMigrateCassandra extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'phone-number:cassandra-migrate';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Migrate phone number from Cassandra to csv';

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
    protected $fileName;
    protected $nextPageToken;
    protected $backPageToken;

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
        $this->fileName = "phone.csv";
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
            $csv = storage_path("cassandra/$this->fileName");
            $output = fopen($csv,"a") or die("Can't open php://output");
            $query = "SELECT * FROM $this->hackRecord";
            $statement = new Cassandra\SimpleStatement($query);
            if(Cache::get('backPageToken')){
                Log::info("Resuming Token");
                $index = Cache::get('index');
                $phoneIndex = Cache::get('phoneIndex');
                Log::info("index: $index");
                Log::info("Phone Index: $phoneIndex");
                $options = array('page_size' => $this->pageSize,'timeout' => $this->cassandraTimeout,'consistency' => Cassandra::CONSISTENCY_LOCAL_QUORUM,'paging_state_token'=>Cache::get('backPageToken'));
            }else{
                Log::info("Fresh Start");
                $index = 1;
                $phoneIndex = 1;
                fputcsv($output, array('phonenumber','email','sourceid','recordid'));
                $options = array('page_size' => $this->pageSize,'timeout' => $this->cassandraTimeout,'consistency' => Cassandra::CONSISTENCY_LOCAL_QUORUM);
            }
            $result = $this->session->execute($statement,new Cassandra\ExecutionOptions($options));
            while (true) {
                Cache::forever('nextPageToken',$result->pagingStateToken());
                //$batch = new Cassandra\BatchStatement();
                foreach ($result as $row) {
                    if ($index % 1000000 == 0) {
                        Log::info("Total record inserted:: $index");
                        Log::info("Total phone record inserted:: $phoneIndex");
                    }
                    $attributes = checkIsEmptyJson($row['attributes']);
                    if(!is_null($attributes)){
                        $attributes = json_decode($attributes);
                        if(is_object($attributes)){
                            if(property_exists($attributes, 'PNUM') && (!is_null($attributes->PNUM) || !empty($attributes->PNUM))){
                                if(preg_match('~[0-9]~', $attributes->PNUM)){
                                    //has numbers
                                    //$data = ['phonenumber'=>$attributes->PNUM,'email'=>$row['email'],'sourceid'=>$row['sourceid']->uuid(),'recordid' => $row['recordid']->uuid()];
                                    fputcsv($output, array($attributes->PNUM,$row['email'],$row['sourceid']->uuid(),$row['recordid']->uuid()));
                                    $phoneIndex++;
                                    Cache::forever('phoneIndex',$phoneIndex);
                                }
                            }
                        }
                    }
                    //$sqlObject[] = $data;
                    $index = $index + 1;
                    Cache::forever('index',$index);
                }
                if ($result->isLastPage()) { break; }
                Cache::forever('backPageToken',Cache::get('nextPageToken'));
                $result = $result->nextPage();
            }
            Log::info("Process Ended.");
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("Cassandra:Phone No Migrate");
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
}
