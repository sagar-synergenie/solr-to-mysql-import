<?php

namespace App\Jobs;

use App\HackRecord;
use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;

class BatchInsertToMysql implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public $sqlObject;
    public function __construct($sqlObject)
    {
        $this->sqlObject = $sqlObject;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        try{
            /*foreach (array_chunk($this->sqlObject, 1000) as $sqlData) {
                HackRecord::insert($sqlData);
            }*/
            HackRecord::create($this->sqlObject);
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("DB:import Error:QueueJob");
            });
        }
    }
}
