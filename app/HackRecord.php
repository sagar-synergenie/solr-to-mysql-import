<?php

namespace App;

use Illuminate\Database\Eloquent\Model;

class HackRecord extends Model
{
    protected $fillable = ['email','sourceid','recordid','attributes','firstname','ipaddress','isdataclean','isremoved','lastname','password','passwordhash','username','status','dateinserted','emaildomain','phonenumber','source_id'];

    public $timestamps = false;
}
