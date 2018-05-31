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
    protected $currentDocument;
    protected $start;
    protected $end;

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
            $start = (int)env('START');
            $end = (int)env('END');
            $csv = storage_path("csv/$this->fileName");
            $output = fopen($csv,"a") or die("Can't open php://output");
            if($start == 0){
                fputcsv($output, array('phonenumber','email','sourceid','recordid'));
            }else{
                $recordFound = $recordFound - $start;
            }
            $this->solrRequest($start,$end,$recordFound,$output);
            Log::info('Process End');
        }catch (\Exception $e){
            $data = "\n\n\n\n\n\n\n\nCurrent Document::\n\n\n\n".json_encode($this->currentDocument)."\n\n\n\n"."START:".$this->start."END:".$this->end;
            Log::critical($e);
            Log::critical($data);
            Mail::raw($e.$data, function ($message){
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
        http://204.12.216.194:8983/solr/hdb_new.hack_records/select?q=PNUM&start=0&rows=10&fl=email%2Csourceid%2Crecordid%2Cattributes&df=attributes&wt=json&indent=true
        $client = new \GuzzleHttp\Client([
            'auth' => [$this->solrUsername, $this->solrPassword],
        ]);
        for($index = 1;$index <= $totalIteration; $index++){
            Log::warning("Iteration::".$index." STARTED ::start::".$start."::end::".$end);
            $this->start = $start;
            $this->end = $end;
            $url = $this->solrURL.$this->hackRecord."/select?q=$entityFilter&start=$start&rows=$end&fl=email%2Csourceid%2Crecordid%2Cattributes&df=$entityType&wt=json&indent=true";
            $response = $client->request("GET", $url);
            $body = $response->getBody();
            // Implicitly cast the body to a string and echo it

            // Explicitly cast the body to a string
            $stringBody = (string) $body;
            $response = json_decode($stringBody);
            dd($response);
            Log::warning('Result Count::'.count($response->response->docs));
            foreach($response->response->docs as $docs){
                $this->currentDocument = $docs;
                if(property_exists($docs, 'attributes') && !is_null($docs->attributes) || !empty($docs->attributes)){
                    $attributes = json_decode($docs->attributes);
                    if(is_object($attributes)){
                        if(property_exists($attributes, 'PNUM') && (!is_null($attributes->PNUM) || !empty($attributes->PNUM))){
                            if(preg_match('~[0-9]~', $attributes->PNUM)){
                                //has numbers
                                fputcsv($output, array($attributes->PNUM,$docs->email,$docs->sourceid,$docs->recordid));
                            }
                        }
                    }
                }
            }
            $start += $end + 1;
            //$end = $end + env('CASSANDRA_PAGE_SIZE');
            Log::warning("Iteration::".$index." ENDED");
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
