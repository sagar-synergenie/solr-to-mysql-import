<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;

class PhoneNumberDataMigrate extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'phone-number:migrate';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Migrate phone number from solr to cassandra';

    /**
     * Create a new command instance.
     *
     * @return void
     */

    protected $solrURL;
    protected $solrUsername;
    protected $solrPassword;
    protected $hackRecord;
    protected $fileName;
    public function __construct()
    {
        parent::__construct();
        $this->solrURL = env('SOLR_URL','');
        $this->solrUsername = env('SOLR_USERNAME','');
        $this->solrPassword = env('SOLR_PASSWORD','');
        $this->hackRecord = "hdb_new.hack_records";
        $this->fileName = "phone.csv";
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        try{
            Log::info('Process Started');
            $start = 0;
            $end = 10;
            $recordFound = $this->solrFirstRequest($start,$end);
            $end = env('CASSANDRA_PAGE_SIZE');
            $csv = storage_path("csv/$this->fileName");
            $output = fopen($csv,"w") or die("Can't open php://output");
            fputcsv($output, array('phonenumber','email','sourceid','recordid'));
            $this->solrRequest($start,$end,$recordFound,$output);
            Log::info('Process End');
        }catch (\Exception $e){
            Log::critical($e);
            Mail::raw($e, function ($message){
                $message->to(env("USER_EMAIL"));
                $message->subject("Phone No script error");
            });
        }
    }

    public function solrRequest($start,$end,$recordFound,$output)
    {
        $totalIteration = ceil($recordFound / $end);
        $entityFilter = "PNUM";
        $entityType = "attributes";
        //"http://204.12.216.194:8983/solr/hdb_new.hack_records/select?q=PNUM&df=attributes&wt=json&indent=true";

        $client = new \GuzzleHttp\Client([
            'auth' => [$this->solrUsername, $this->solrPassword],
        ]);
        for($index = 1;$index <= $totalIteration; $index++){
            Log::warning("Iteration::".$index."STARTED ::start::".$start."::end::".$end);
            $url = $this->solrURL.$this->hackRecord."/select?q=$entityFilter&start=$start&rows=$end&df=$entityType&wt=json&indent=true";
            $response = $client->request("GET", $url);
            $body = $response->getBody();
            // Implicitly cast the body to a string and echo it

            // Explicitly cast the body to a string
            $stringBody = (string) $body;
            $response = json_decode($stringBody);
            foreach($response->response->docs as $docs){
                if(property_exists($docs, 'attributes') && !is_null($docs->attributes) || !empty($docs->attributes)){
                    $attributes = json_decode($docs->attributes);
                    if(is_object($attributes) || is_array($attributes)){
                        if(!is_null($attributes->PNUM) || !empty($attributes->PNUM)){
                            if(preg_match('~[0-9]~', $attributes->PNUM)){
                                //has numbers
                                fputcsv($output, array($attributes->PNUM,$docs->email,$docs->sourceid,$docs->recordid));
                            }
                        }
                    }
                }
            }
            $start = $end + 1;
            $end = $end + env('CASSANDRA_PAGE_SIZE');
            Log::warning("Iteration::".$index."END ::start::".$start."::end::".$end);
        }
        fclose($output);
    }

    public function solrFirstRequest($start,$end)
    {
        $entityFilter = "PNUM";
        $entityType = "attributes";
        //"http://204.12.216.194:8983/solr/hdb_new.hack_records/select?q=PNUM&df=attributes&wt=json&indent=true";
        $url = $this->solrURL.$this->hackRecord."/select?q=$entityFilter&start=$start&rows=$end&df=$entityType&wt=json&indent=true";
        $client = new \GuzzleHttp\Client([
            'auth' => [$this->solrUsername, $this->solrPassword],
        ]);
        $response = $client->request("GET", $url);
        $body = $response->getBody();
        // Implicitly cast the body to a string and echo it

        // Explicitly cast the body to a string
        $stringBody = (string) $body;
        $response = json_decode($stringBody);

        $recordFound = $response->response->numFound;
        return $recordFound;
    }
}
