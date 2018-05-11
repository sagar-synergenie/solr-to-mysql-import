<?php
function checkIsEmpty($value)
{
    $value = trim($value);
    if(!is_null($value) && strlen($value) > 0){
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
    if(!is_null($value) && strlen($value) > 0){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }else {
            if($value == 'TRUE' || $value == TRUE){
                return TRUE;
            }elseif($value == 'FALSE' || $value == FALSE){
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
    if(!is_null($value) && strlen($value) > 0 && is_object(json_decode($value))){
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