<?php

namespace App;

use Illuminate\Database\Eloquent\Model;

class MissingHackSourceRecord extends Model
{
    protected $table = 'missing_hack_source_records';

    protected $fillable = ['email','sourceid','recordid','attributes','firstname','ipaddress','isdataclean','isremoved','lastname','password','passwordhash','username','status','dateinserted','emaildomain','phonenumber'];

    public $timestamps = false;
}
