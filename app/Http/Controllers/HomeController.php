<?php

namespace App\Http\Controllers;

use Heroic\Scanner\Helpers\Scanner\CassandraQuery;
use Heroic\Scanner\Models\Scanner\ScannerRequest;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

class HomeController extends Controller
{
    /**
     * Create a new controller instance.
     *
     * @return void
     */
    public function __construct()
    {
        $this->middleware('auth');
    }

    /**
     * Show the application dashboard.
     *
     * @return \Illuminate\Http\Response
     */
    public function index()
    {
        Cache::forget('datatable_previous_page');
        Cache::forget('cassandra_pagination');
        return view('home');
    }

    public function jsonData(Request $request)
    {
        $expiresAt = now()->addMinutes(120);
        $requestData = $request->all();
        $firstRequest = false;
        $forward = false;
        $backward = false;
        if(is_null(Cache::get('datatable_previous_page'))){
            $firstRequest = true;
            Cache::put('datatable_previous_page',$requestData['start'],$expiresAt);
        }else{
            if($requestData['start'] > Cache::get('datatable_previous_page')){
                $forward = true;
            }else{
                $backward = true;
            }
            Cache::put('datatable_previous_page',$requestData['start'],$expiresAt);
        }

        $entityType = "email";
        $entityFilter = "sagar@gmail.com";
        $response = $this->getData($entityType,$entityFilter,$firstRequest,$forward,$backward);
        $cassandraPagination = Cache::get('cassandra_pagination');
        $total = $cassandraPagination['records'];
        /*if(is_null($cassandraPagination['next'])){
            $total = 10;
        }*/
        $index = 1;
        foreach($response as $res){
            $record['index'] = $index;
            $record['email'] = $res['email'];
            $record['sourceid'] =$res['sourceid'];
            $record['emaildomain'] = $res['emaildomain'];
            $record['status'] = $res['status'];
            $record['isdataclean'] = $res['isdataclean'];
            $data[] = array_values($record);
            $index++;
        }
        $finalData['draw'] = $requestData['draw'];
        $finalData['recordsTotal'] = $total;
        $finalData['iTotalRecords'] = $total;
        $finalData['iTotalDisplayRecords'] = $total;
        $finalData['recordsFiltered'] = $total;
        $finalData['aaData'] = $data;
        return $finalData;
    }

    public function getData($entityType, $entityFilter,$firstRequest,$forward,$backward)
    {
        try{
            $expiresAt = now()->addMinutes(120);
            $scannerRequest = new ScannerRequest();
            $scannerRequest->addEntityFilter($entityType, $entityFilter);
            $cassandraObject = new CassandraQuery($scannerRequest);
            $cassandraPagination = Cache::get('cassandra_pagination');
            if($firstRequest){
                if(is_null($cassandraPagination)){
                    $indexArray = ['index'=>[1=>null]];
                    ksort($indexArray);
                    Cache::put('cassandra_pagination',$indexArray,$expiresAt);
                }
                $response = $cassandraObject->fetchHackRecords(10,$entityType,$entityFilter);
                $cassandraPagination = Cache::get('cassandra_pagination');
                if(is_null($response['token'])) {
                    $cassandraPagination['next'] = null;
                    $cassandraPagination['previous'] = 0;
                    $cassandraPagination['records'] = count($response['array']);
                }else{
                    $nextKey = key($cassandraPagination['index']) + 1;
                    $cassandraPagination['next'] = $nextKey;
                    $cassandraPagination['previous'] = null;
                    $cassandraPagination['index'][$nextKey] = $response['token'];
                    $cassandraPagination['records'] = count($response['array']) + 10;
                }
                ksort($cassandraPagination['index']);
                Cache::put('cassandra_pagination',$cassandraPagination,$expiresAt);
            }else{
                $cassandraPagination = Cache::get('cassandra_pagination');
                if($forward){
                    $token = $cassandraPagination['index'][$cassandraPagination['next']];
                }
                if($backward){
                    $token = $cassandraPagination['index'][$cassandraPagination['previous']];
                }
                $response = $cassandraObject->fetchHackRecords(10,$entityType,$entityFilter,$token);
                if(is_null($response['token'])){
                    $cassandraPagination['records'] = $cassandraPagination['records'];
                    $cassandraPagination['next'] = null;
                    ksort($cassandraPagination['index']);
                    end($cassandraPagination['index']);
                    if(is_null($cassandraPagination['previous'])){
                        $cassandraPagination['previous'] = null;
                    }else{
                        $cassandraPagination['previous'] = key($cassandraPagination['index']) - 1;
                    }
                }else{
                    if($forward){
                        $cassandraPagination['records'] += count($response['array']);
                        ksort($cassandraPagination['index']);
                        end($cassandraPagination['index']);
                        $nextKey = key($cassandraPagination['index']) + 1;
                        $cassandraPagination['next'] = $nextKey;
                        if(is_null($cassandraPagination['previous'])){
                            $cassandraPagination['previous'] = 1;
                        }else{
                            $cassandraPagination['previous'] = $nextKey - 1;
                        }
                        $cassandraPagination['index'][$nextKey] = $response['token'];
                    }
                    if($backward){
                        $nextKey = $cassandraPagination['previous'] + 1;
                        if(($nextKey - 1) > 1){
                            $cassandraPagination['previous'] = $nextKey - 2;
                            if(is_null($cassandraPagination['next'])){
                                $cassandraPagination['records'] = $cassandraPagination['records'];
                            }else{
                                $cassandraPagination['records'] -= count($response['array']);
                            }
                        }else{
                            $cassandraPagination['records'] = count($response['array']) + 10;
                            $cassandraPagination['previous'] = null;
                        }
                        $cassandraPagination['next'] = $nextKey;
                    }
                }
                ksort($cassandraPagination['index']);
                Cache::put('cassandra_pagination',$cassandraPagination,$expiresAt);
            }
            Log::info("::next::".$cassandraPagination['next']."::previous::".$cassandraPagination['previous']."::records::".$cassandraPagination['records']);
            return $response['array'];
        }catch (\Exception $e){
            Log::info($e);
            dd($e);
        }
    }
}
