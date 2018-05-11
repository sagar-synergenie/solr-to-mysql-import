<?php

namespace App;

use Illuminate\Database\Eloquent\Model;

class MissingHackSourceRecord extends Model
{
    protected $table = 'missing_hack_source_records';
    
    protected $fillable = ['email','sourceid','recordid'];
}
