<?php

namespace App;

use Illuminate\Database\Eloquent\Model;

class MissingHackSourceRecord extends Model
{
    protected $fillable = ['email','sourceid','recordid'];
}
