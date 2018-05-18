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
            return 0;
        }elseif($value == ""){
            return 0;
        }else {
            if($value == 'TRUE' || $value == TRUE){
                return 1;
            }elseif($value == 'FALSE' || $value == FALSE){
                return 0;
            }
        }
    }else{
        return 0;
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
        }elseif(is_object(json_decode($value))) {
            return json_encode(json_decode($value));
        }else{
            return null;
        }
    }else{
        return null;
    }
}

function checkIsEmptyPassword($value)
{
    $value = trim($value);
    if(!is_null($value) && strlen($value) > 0){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }elseif(strlen($value) > 1000) {
            return null;
        }elseif(strpos($value, '\\') !== false || strpos($value, '?') !== false){
            return null;
        }else{
            return $value;
        }
    }else{
        return null;
    }
}
function checkIsEmptyUsername($value)
{
    $value = trim($value);
    if(!is_null($value) && strlen($value) > 0){
        if($value == "{}"){
            return null;
        }elseif($value == ""){
            return null;
        }elseif(strpos($value, '\\') !== false || strpos($value, '?') !== false) {
            return null;
        }else{
            return $value;
        }
    }else{
        return null;
    }
}
