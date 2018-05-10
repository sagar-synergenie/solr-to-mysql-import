<?php
function checkIsEmpty($value)
{
    $value = trim($value);
    if(!empty($value) || !is_null($value) || $value != ""){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }else {
            return $value;
        }
    }else{
        return null;
    }
}

function checkIsEmptyConvertBoolean($value)
{
    $value = trim($value);
    if(!empty($value) || !is_null($value)){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }else {
            if($value == 'TRUE'){
                return TRUE;
            }elseif($value == 'FALSE'){
                return FALSE;
            }
        }
    }else{
        return null;
    }
}

function checkIsEmptyAndRetrievePhone($value)
{
    $value = trim($value);
    if(!empty($value) || !is_null($value)){
        if($value == "{}"){
            return null;
        }else{
            $value = json_decode($value);
            if (property_exists($value, 'PNUM')){
                return $value->PNUM;
            }
        }
    }else{
        return null;
    }
}

function checkIsEmptyJson($value)
{
    $value = trim($value);
    if(!empty($value) || !is_null($value) || $value != ""){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }else {
            return json_encode(json_decode($value));
        }
    }else{
        return null;
    }
}