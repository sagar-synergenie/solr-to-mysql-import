<?php

namespace App\Console\Commands;

use App\HackRecord;
use App\User;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;
use Maatwebsite\Excel\Facades\Excel;

class SolrToMysqlImport extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'mysql:import';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Export data from solr and import to mysql';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    protected $fileName;

    public function __construct()
    {
        parent::__construct();

        $this->fileName = env('CSV_FILE_NAME');
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        try{
            $this->getDataFromCSV();
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Error");
            });
        }
    }

    public function getDataFromCSV()
    {
        Log::info('Process Started');
        $sqlObject = array();
        $recordCount = 0;
        $chunkSize = (int)env("CHUNK_SIZE");
        Excel::filter('chunk')->load($this->fileName)->chunk($chunkSize, function($results) use (&$recordCount,$chunkSize)
        {
            Log::info('Inside Chunk Function');
            foreach($results as $row)
            {
                //echo "<pre>";print_r($row->email);
                // do stuff
                $data = [
                    'email' => checkIsEmpty($row->email),
                    'sourceid' => checkIsEmpty($row->sourceid),
                    'recordid' => checkIsEmpty($row->recordid),
                    'attributes' => checkIsEmptyJson($row->attributes),
                    'firstname' => checkIsEmpty($row->firstname),
                    'ipaddress' => checkIsEmpty($row->ipaddress),
                    'isdataclean' => checkIsEmptyConvertBoolean($row->isdataclean),
                    'isremoved' => checkIsEmptyConvertBoolean($row->isremoved),
                    'lastname' => checkIsEmpty($row->lastname),
                    'password' => checkIsEmpty($row->password),
                    'passwordhash' => checkIsEmpty($row->passwordhash),
                    'username' => checkIsEmpty($row->username),
                    'status' => checkIsEmpty($row->status),
                    'dateinserted' => date('Y-m-d H:i:s'),
                    'emaildomain' => checkIsEmpty($row->emaildomain),
                    'phonenumber' => checkIsEmptyAndRetrievePhone($row->attributes),
                ];
                $recordCount = $recordCount + $chunkSize;

                if($recordCount % 500000 == 0){
                    Mail::raw("Total record inserted:: $recordCount", function ($message){
                        $message->to(env("USER_EMAIL"));
                        $message->subject("DB:import Record Count");
                    });
                    Log::info("Total record inserted:: $recordCount");
                }
                $sqlObject[] = $data;
            }
            HackRecord::insert($sqlObject);
            $sqlObject = array();
        },false);
        Log::info('Process End!');
    }
}
