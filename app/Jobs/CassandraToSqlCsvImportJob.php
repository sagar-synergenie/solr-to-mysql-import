<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;

class CassandraToSqlCsvImportJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    protected $fileName;
    protected $index;

    public function __construct($fileName,$index)
    {
        $this->fileName = $fileName;
        $this->index = $index;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        try{
            $csv = storage_path("csv/$this->fileName");
            $query = sprintf("LOAD DATA local INFILE '%s' 
                                    IGNORE  
                                    INTO TABLE hack_records 
                                    FIELDS TERMINATED BY ';' 
                                    IGNORE 1 LINES  
                                    (`source_id`, `email`, `emaildomain`, `username`, `firstname`, `lastname`, `password`, `passwordhash`, `ipaddress`, `phonenumber`, `attributes`, `status`, `isdataclean`, `isremoved`, `dateinserted`) 
                                    SET dateinserted = NOW()",
                addslashes($csv));

            DB::connection()->getpdo()->exec($query);

            Mail::raw("Total record inserted:: $this->index", function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Record Count ");
            });
            Log::info("Total record inserted:: $this->index");
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Error Queue");
            });
        }
    }
}
