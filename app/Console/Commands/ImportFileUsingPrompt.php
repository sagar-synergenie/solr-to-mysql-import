<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class ImportFileUsingPrompt extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'cassandra:import-prompt {file}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Import single csv';

    /**
     * Create a new command instance.
     *
     * @return void
     */

    protected $fileName;
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */

    public function handle()
    {
        try{
            $this->fileName = $this->argument('file');
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

            /*Mail::raw("Total record inserted:: $this->index", function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Record Count ");
            });*/
            $this->line('success');
        }catch (\Exception $e){
            $this->line($e);
            Log::critical($e);
            /*$e = $e."\n\nFileName::$this->fileName\n\n::Index Number::$this->index";
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Error Queue");
            });*/
        }
    }
}
