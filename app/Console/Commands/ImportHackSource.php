<?php

namespace App\Console\Commands;

use App\HackSource;
use Illuminate\Console\Command;
use Cassandra;

class ImportHackSource extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'cassandra-sql:source-import';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Hack Source import';

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

    public function __construct()
    {
        parent::__construct();

        $this->cassandraURL = explode(",",env('CASSANDRA_URL','127.0.0.1'));
        $this->cassandraUsername = env('CASSANDRA_USERNAME','');
        $this->cassandraPassword = env('CASSANDRA_PASSWORD','');
        $this->cassandraPort = (int)env('CASSANDRA_PORT',9042);
        $this->keySpace = env('CASSANDRA_KEYSPACE','hdb_new');;
        $this->cassandraTimeout = 600;
        $this->hackRecord = "hack_source";
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
        $query = "SELECT * FROM $this->hackRecord";
        $statement = new Cassandra\SimpleStatement($query);
        $result = $this->session->execute($statement);
        foreach($result as $row){
            if(!empty($row['dateleaked'])){
                $timeStamp = (array)$row['dateleaked'];
                $row['dateleaked'] = date('Y-m-d H:i:s',$timeStamp['seconds']);
            }
            if(!empty($row['dateinserted'])){
                $timeStamp = (array)$row['dateinserted'];
                $row['dateinserted'] = date('Y-m-d H:i:s',$timeStamp['seconds']);
            }
            if(!empty($row['date_files_found'])){
                $timeStamp = (array)$row['date_files_found'];
                $row['date_files_found'] = date('Y-m-d H:i:s',$timeStamp['seconds']);
            }
            if(!empty($row['date_updated'])){
                $timeStamp = (array)$row['date_updated'];
                $row['date_updated'] = date('Y-m-d H:i:s',$timeStamp['seconds']);
            }
            if(!empty($row['date_uploaded_redshift'])){
                $timeStamp = (array)$row['date_uploaded_redshift'];
                $row['date_uploaded_redshift'] = date('Y-m-d H:i:s',$timeStamp['seconds']);
            }
            $data[] = $row;
        }
        HackSource::insert($data);
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
