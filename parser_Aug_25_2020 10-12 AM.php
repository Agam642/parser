<?php
    include 'config.php';
    date_default_timezone_set('UTC');
    class Record{
        //number of columns in each type of csv file
        const airportColumns = 25;
        const runwayColumns = 51;

        //used to identify each record line by line in isAirportorRunway function
        const isAirport = "air";
        const isRunway = "run";
        const isHeader = "header";

        //position of latitude and longitude DMS
        const pos_LatitudeDirection = 0;
        const pos_LatitudeDegree = 1;
        const pos_LatitudeMin = 3;
        const pos_LatitudeSec = 5;
        const pos_LongitudeDirection = 0;
        const pos_LongitudeDegree = 1;
        const pos_LongitudeMin = 4;
        const pos_LongitudeSec = 6;

        //length of latitude and longitude DMS
        const length_LatitudeDirection = 1;
        const length_LatitudeDegree = 2;
        const length_LatitudeMin = 2;
        const length_LatitudeSec = 4;
        const length_LongitudeDirection = 1;
        const length_LongitudeDegree = 3;
        const length_LongitudeMin = 2;
        const length_LongitudeSec = 4;

        //used for DMS to decimal degrees formula
        const minutesInHour = 60;
        const secondsInHour = 3600;

        //conversion factior used for converting from feet to meters
        const feetInAMeter = 3.2808;
        
        //position of DataType and ICAO in csv files
        const pos_DataType = 0;
        const pos_ICAO = 2;

        //to return enum when a record is found to be identical to database already
        const unchangedRecord = "Unchanged";
        const invalidRecord = "Invalid";

        //update fields used in sql commands
        const updatedBy = "NavData Import";
        const changeReason = "NavData Import";
        const changeImpact = "FS";

        //to identify diff files
        const beforeDiff = "before";
        const afterDiff = "after";

        const debugMode = "debug";

        //static variable to know if airport or runway
        public static $isRunway;

        //static variable to connect to database
        public static $connectionString;

        //static variables to help write to logfiles
        public static $errorLog;
        public static $recordLog;
        public static $redFlagsLog;
        public static $DatabaseDumpLog_before;
        public static $DatabaseDumpLog_after;
        public static $DatabaseDiffLog;
        public static $DatabaseDiffBaselineLog;
        public static $DatabaseDiffChangesLog;
        

        function __construct($record)
        {
            $this->record = $record;
        }
        function getField($fieldNum)
        {
            return $this->record[$fieldNum];
        }
        static function isAirportOrRunwayRecord($record)
        {
            $count = count($record);
            //Just make sure it is not a header row by checking one of the column headers
            if($record[3] != "ICAO CODE")
            {
                if( $count == Self::airportColumns)
                {
                    //Is a airport
                    Self::$isRunway = False;
                    return Self::isAirport;
                }
                else if( $count == Self::runwayColumns )
                {
                    //Is a runway;
                    Self::$isRunway = True;
                    return Self::isRunway;
                }
            }
            else
            {
                return Self::isHeader;
            }
        }
        static function DMSLatitude($original)
        {
            //store dms and direction
            $direction = substr($original, Self::pos_LatitudeDirection, Self::length_LatitudeDirection);
            $degree = substr($original, Self::pos_LatitudeDegree, Self::length_LatitudeDegree);
            $min = substr($original, Self::pos_LatitudeMin, Self::length_LatitudeMin);
            $sec = substr($original, Self::pos_LatitudeSec, Self::length_LatitudeSec);
            $sec = floatval(substr($sec, 0, 2) . "." . substr($sec, 2, 2));

            //convert to decimal degrees
            $decimal = Self::DMS_to_decimal($degree, $min, $sec);

            //handle direction
            if ($direction == "S")
            {
                $decimal = -1 * abs($decimal);
            }
            return floatval($decimal);
        }
        static function DMSLongitude($original)
        {
            //store dms and direction
            $direction = substr($original, Self::pos_LongitudeDirection, Self::length_LongitudeDirection);
            $degree = substr($original, Self::pos_LongitudeDegree, Self::length_LongitudeDegree);
            $min = substr($original, Self::pos_LongitudeMin, Self::length_LongitudeMin);
            $sec = substr($original, Self::pos_LongitudeSec, Self::length_LongitudeSec);
            $sec = floatval(substr($sec, 0, 2) . "." . substr($sec, 2, 2));
            
            //convert to decimal degrees
            $decimal = Self::DMS_to_decimal($degree, $min, $sec);
    
            //handle direction
            if ($direction == "W")
            {
                $decimal = -1 * abs($decimal);
            }
            return floatval($decimal);
        }
        static function DMS_to_decimal($degree, $min, $sec)
        {
            // return dms converted to decimal degrees
            $decimalDegrees = $degree + ($min/Self::minutesInHour) + ($sec/Self::secondsInHour);
            return $decimalDegrees;
        }
        function feetToMeters($numberInFeet)
        {
            if (is_numeric($numberInFeet))
            {
                //convert feet to meters and return
                $numberInMeters = round($numberInFeet/Self::feetInAMeter);
                return $numberInMeters;
            }
        }
        static function connectDatabase()
        {
            // Connecting to the APEx Database
            $conn = mysqli_connect($GLOBALS['connectionAPExHost'], $GLOBALS['connectionAPExUsername']);
            Self::$connectionString = $conn;
            if(!$conn)
            {
                echo "Script exited could not connect to database check error.log\n";
                $error_message = "An error connecting to the APEx Database occured, adjust config file\n";
                // logging error message to given log file 
                fwrite(Self::$errorLog, $error_message);
                exit;
            }
            echo "Successfully connected to APEx DB\n"."<br>";

            //select a database to work with
            $selected = mysqli_select_db($conn, $GLOBALS['connectionAPExDB']);

            if(!$selected) 
            {
                echo "Script exited could not select database check error.log\n";
                $error_message = "Could not select database, adjust config file\n"; 
                // logging error message to given log file 
                fwrite(Self::$errorLog, $error_message);
                exit;
            } 
            echo "Successfully selected " . $GLOBALS['connectionAPExDB'] ."\n<br>"; 
        }
        function getDataType()
        {
            return $this->getField(Self::pos_DataType); 
        }
        function getICAO()
        {         
            return $this->getField(Self::pos_ICAO);
        }
        //query database to find intersecting icaos
        static function queryData($ICAO)
        { 
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);
            $conn = Self::$connectionString;
            //query proper table depending on passed in datatype
            if (Self::$isRunway == False)
            {
                $sql="SELECT * FROM Airport WHERE ICAO in ('$ICAO_string')";
            }
            else
            {
                $sql="SELECT * FROM MainRunway WHERE ICAO in ('$ICAO_string')";
            }
            
            $result = mysqli_query($conn, $sql);

            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                //ICAO_Intersection array to intersection between database
                $ICAO_Intersection = array();
                while($row = mysqli_fetch_assoc($result)) 
                {  
                    //loop through all icaos
                    for ($i = 0; $i < count($ICAO); $i++)  
                    {
                        $icao = $row["ICAO"];
                        //if current parsed icao matches apex
                        if ($ICAO[$i] == $icao)
                        {
                            //set up structure of $ICAO_Intersection array with intial values
                            if (!array_key_exists($icao, $ICAO_Intersection))
                            {
                                if (Self::$isRunway == False)
                                {
                                    $ICAO_Intersection[$icao] = array("ICAO" => $icao, "Elevation" => $row["Elevation"],
                                    $row["Latitude"], $row["Longitude"], $row["MagnVariation"], $row["Aerodrome"]);
                                }
                                else
                                {
                                    $runway = $row["MainRunway"];
                                    //set up structure of array and insert one runway item
                                    $ICAO_Intersection[$icao][$runway] = array("MainRunway" => $runway, "MagnHeading" => $row["MagnHeading"],
                                    "StartLatitude" => $row["StartLatitude"], "StartLongitude" => $row["StartLongitude"], "TrueHeading" => $row["TrueHeading"],
                                    "TORA" => $row["TORA"], "TODA" => $row["TODA"], "LDA" => $row["LDA"], "ASDA" => $row["ASDA"]);
                                }
                            }
                            else
                            {
                                //if ICAO repeats than add more runway entries
                                if (Self::$isRunway == True)
                                {
                                    $runway = $row["MainRunway"];
                                    
                                    $ICAO_Intersection[$icao][$runway] = array("MainRunway" => $runway, "MagnHeading" => $row["MagnHeading"],
                                    "StartLatitude" => $row["StartLatitude"], "StartLongitude" => $row["StartLongitude"], "TrueHeading" => $row["TrueHeading"],
                                    "TORA" => $row["TORA"], "TODA" => $row["TODA"], "LDA" => $row["LDA"], "ASDA" => $row["ASDA"]);
                                }
                            }                            
                        }
                    }
                }
            } 
            else 
            {
                echo "no matches for ICAO found\n"."<br>";
                $error_message = "0 results Found searching for matching ICAO\n";
                fwrite(Self::$errorLog, $error_message);
                exit;
            }
            return $ICAO_Intersection;
        }
        static function recordDatabase($result, $logfile, $tableName)
        {
            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                while ($row = mysqli_fetch_row($result)) 
                { 
                    for ($i = 0; $i < count($row); $i++)  
                    {
                        if ($i == (count($row)-1))
                        {
                            $record = $row[$i] . "\n";
                        }
                        else if ($i == 0)
                        {
                            $record = "('$tableName') -> " . $row[$i] . " ";
                        }
                        else
                        {
                            $record = $row[$i] . " ";
                        }
                        fwrite($logfile, $record);
                    }
                }
            }
        }
        static function dumpRunways($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM Runway WHERE ICAO in ('$ICAO_string') AND Intersection = 'N'";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "Runway");
        }
        static function dumpIntersections($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM Runway WHERE ICAO in ('$ICAO_string') AND Intersection = 'Y'";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "Intersection");
        }
        static function dumpMainRunway($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM MainRunway WHERE ICAO in ('$ICAO_string')";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "MainRunway");
        }
        static function dumpAirport($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM Airport WHERE ICAO in ('$ICAO_string')";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "Airport");
        }
        static function dumpObstacles($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM Obstacle WHERE ICAO in ('$ICAO_string')";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "Obstacle");
        }
        static function dumpEngineFailure($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM EngineFailure WHERE ICAO in ('$ICAO_string')";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "EngineFailure");
        }
        static function dumpEngineFailureObstacle($conn, $ICAO_string, $logfile)
        {
            $sql = "SELECT * FROM EngineFailureObstacle WHERE ICAO in ('$ICAO_string')";
            $result = mysqli_query($conn, $sql);
            Self::recordDatabase($result, $logfile, "EngineFailureObstacle");
        }
        //add something in config file to signify if database dump should run
        static function databaseDump($ICAO, $trigger)
        {
            //check which text file to write to
            $logfile;
            if ($trigger == Record::beforeDiff)
            {
                $filePath = $GLOBALS['fileFolderLocation'] . "Database_Dump_before.txt";
                Record::$DatabaseDumpLog_before = fopen($filePath, "w");
                if(!Record::$DatabaseDumpLog_before)
                {
                    echo "An error opening Database_Dump.log file occured\n";
                }
                $logfile = Self::$DatabaseDumpLog_before;
            }
            else if ($trigger == Record::afterDiff)
            {
                $filePath = $GLOBALS['fileFolderLocation'] . "Database_Dump_after.txt";
                Record::$DatabaseDumpLog_after = fopen($filePath, "w");
                if(!Record::$DatabaseDumpLog_after)
                {
                    echo "An error opening Database_Dump.log file occured\n";
                }
                $logfile = Self::$DatabaseDumpLog_after;
            }

            $conn = Self::$connectionString;
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);

            Self::dumpRunways($conn, $ICAO_string, $logfile);
            Self::dumpIntersections($conn, $ICAO_string, $logfile);
            Self::dumpMainRunway($conn, $ICAO_string, $logfile);
            Self::dumpAirport($conn, $ICAO_string, $logfile);
            Self::dumpObstacles($conn, $ICAO_string, $logfile);
            Self::dumpEngineFailure($conn, $ICAO_string, $logfile);
            Self::dumpEngineFailureObstacle($conn, $ICAO_string, $logfile);
        }
        static function databaseDiff()
        {
            $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff_baseline.txt";
            if (file_exists($filePath)) 
            {
                $DatabaseDiff = $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff.txt";
                $DatabaseDiffBaseline = $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff_baseline.txt";
                copy($DatabaseDiff, $DatabaseDiffBaseline); 
            }
            $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff.txt";
            Record::$DatabaseDiffLog = fopen($filePath, "w");
            if(!Record::$DatabaseDiffLog)
            {
                echo "An error opening Database_Dump.log file occured\n";
            }
            $dump_before = file("Database_Dump_before.txt");
            $dump_after = file("Database_Dump_after.txt");

            $diff = array_diff($dump_after, $dump_before);

            foreach($diff as $line)
            {
                fwrite(Record::$DatabaseDiffLog , $line);
            }
        }
        static function databaseDiffFromLastRun()
        {
            $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff_baseline.txt";

            if (file_exists($filePath)) 
            {
                $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff_changes.txt";
                Record::$DatabaseDiffChangesLog = fopen($filePath, "w");
                if(!Record::$DatabaseDiffChangesLog)
                {
                    echo "An error opening Database_Dump.log file occured\n";
                }

                $diff_before = file("Database_diff_baseline.txt");
                $diff_after = file("Database_diff.txt");

                $diff = array_diff($diff_after, $diff_before);

                foreach($diff as $line)
                {
                    fwrite(Record::$DatabaseDiffChangesLog , $line);
                }

            }
            else
            {
                $DatabaseDiff = $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff.txt";
                $DatabaseDiffBaseline = $filePath = $GLOBALS['fileFolderLocation'] . "Database_diff_baseline.txt";
                copy($DatabaseDiff, $DatabaseDiffBaseline); 
            }
            /*
            fclose($DatabaseDumpLog_after);
            fclose($DatabaseDumpLog_before);
            */
        }
        
    }
    //Airport records
    class Airport extends Record
    {
        //position of MagneticVariation Items
        const pos_MagneticVariationDirection = 0;
        const pos_MagneticVariationDegree = 1;
        const pos_MagneticVariationDecimal = 4;
        //length of MagneticVariation Items
        const length_MagneticVariationDirection = 1;
        const length_MagneticVariationDegree = 3;
        const length_MagneticVariationDecimal = 1;

        //positions defined to contain corresponding information
        const pos_IATA = 4;
        const pos_Latitude = 8;
        const pos_Longitude = 9;
        const pos_MagneticVariation = 10;
        const pos_Elevation = 11;
        const pos_AirportName = 22;
        const pos_Cycle = 23;

        //enum values used in compare elevation function
        const elevationHigher = "higher";
        const elevationEqual = "equal";
        const elevationLower = "lower";

        const releasedStatus = "Y";

        function __construct($apRecord)
        {
            parent::__construct($apRecord);
        }
        function getIATA()
        {
            return $this->getField(Self::pos_IATA); 
        }
        function getLatitude()
        {
            //store orginal field
            $original = $this->getField(Self::pos_Latitude);
            if (is_numeric(substr($original, 1)))
            {
                //perform dms to decimal degrees operation
                $latitude = round($this->DMSLatitude($original), 9);

                return number_format($latitude, 10);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . " - Could not parse Latitude invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . " - Could not parse Latitude invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getLongitude()
        {
            //store orginal field
            $original = $this->getField(Self::pos_Longitude);
            if (is_numeric(substr($original, 1)))
            {
                //perform dms to decimal degrees operation
                $longitude = round($this->DMSLongitude($original), 9);
                return number_format($longitude, 10);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . " - Could not parse Longitude invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . " - Could not parse Longitude invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getMagneticVariation()
        {
            //store needed extracted data
            $original = $this->getField(Self::pos_MagneticVariation);
            $direction = substr($original, Self::pos_MagneticVariationDirection, Self::length_MagneticVariationDirection);
            $degree = substr($original, Self::pos_MagneticVariationDegree, Self::length_MagneticVariationDegree);
            $decimal = substr($original, Self::pos_MagneticVariationDecimal, Self::length_MagneticVariationDecimal);

            if (is_numeric(substr($original, 1)) OR $original == "0")
            {
                //concatanate
                $result = floatval($degree . "." . $decimal . "0000");

                //set to negative if East direction
                if ($direction == "E")
                {
                    $result = -1 * abs($result);
                }
                //return with 4 decimal points to match apex
                return number_format($result, "4");
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . " - Could not parse MagneticVariation invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . " - Could not parse MagneticVariation invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getAirportElevation()
        {
            $elevation = $this->getField(Self::pos_Elevation);
            if (is_numeric($elevation) OR $elevation == "0")
            {
                return doubleval($elevation);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . " - Could not parse AirportElevation invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . " - Could not parse AirportElevation invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getAirportName()
        {
            $conn = Self::$connectionString;
            $name = $this->getField(Self::pos_AirportName);
            return $name;
        }
        function getCycle()
        {
            return $this->getField(Self::pos_Cycle);
        }
        function compareFields($ICAO)
        {
            //set an array with parsed data
            $parsedData = array("MagnVariation" => $this->getMagneticVariation(),
            "Latitude" => $this->getLatitude(), "Longitude" => $this->getLongitude(), 
            "Elevation" => $this->getAirportElevation(), "Aerodrome" => $this->getAirportName());

            $error = false;
            foreach ($parsedData as $field => $value)
            {
                if ($value == NULL)
                {
                    $txt = "ICAO: " . $this->getICAO() . "  Field: " . $field . " - Invalid input at this field, update for this record skipped\n";
                    fwrite(Self::$redFlagsLog, $txt);
                    $error = true;
                }
            }
            if ($error == true)
            {
                return Self::invalidRecord;
            }
            $result = array_diff($parsedData, $ICAO);
            if (empty($result)) 
            {
                // list is empty. There are no differences
                return Self::unchangedRecord;
            }
            if ((count($result) == 1) AND ($parsedData["Elevation"] < $ICAO["Elevation"]))
            {
                return Self::unchangedRecord;
            }
            
            $keys = array_keys($result);

            //create string of changed fields to be returned
            $updatedFields = implode(", ", $keys);

            return $updatedFields;
        }
        //function to check if parsed elevation is higher than apex
        function compareElevation($apexElevation, $parsedElevation) 
        {
            //check if parsed elevation higher or lower or equal to apex
            if ($parsedElevation > $apexElevation)
            {
                return Self::elevationHigher;
            }
            else if ($parsedElevation < $apexElevation)
            {
                return Self::elevationLower;
            }
            //if equal
            return Self::elevationHigher;
        }
        function updateAirport($ICAO)
        {  
            $conn = Self::$connectionString;
            //store all data needed to be updated      
            $icao = $this->getICAO();
            $iata = $this->getIATA();
            $latitude = $this->getLatitude();
            $longitude = $this->getLongitude();
            $magnvariation = $this->getMagneticVariation();
            $elevation = $this->getAirportElevation();
            $airportName = $this->getAirportName();
            $airportName = mysqli_real_escape_string($conn, $airportName);
            $updated = date("Y-m-d H:i:s");
            $updatedBy = Runway::updatedBy;
            $changeReason = Runway::changeReason;
            $changeImpact = Runway::changeImpact;
            $fieldComparison = $this->compareFields($ICAO);
            $releasedStatus = Self::releasedStatus;

            //check for already up to date record
            if ($fieldComparison == Self::unchangedRecord)
            {
                //need to log here that there is no update required for this record
                $txt = "ICAO: " . $icao . "  Table: Airport table" . " - This airport is already up to date according to parsed data\n";
                echo $txt;
                fwrite(Self::$redFlagsLog, $txt);
                return;
            }
            else if ($fieldComparison == Self::invalidRecord)
            {
                //skip this record if there is an error (already logged in compareFields)
                return;
            }
            //compare elevation
            if ($this->compareElevation($ICAO["Elevation"], $elevation) == Self::elevationHigher)
            {
                // if elevation is higher than apex then update
                //sql update statment
                $sql = "UPDATE Airport 
                SET  Latitude = ('$latitude'), 
                    Longitude = ('$longitude'),
                MagnVariation = ('$magnvariation'),
                    Elevation = ('$elevation'),
                    Aerodrome = ('$airportName'),
                      Updated = ('$updated'),
                    UpdatedBy = ('$updatedBy'),
                 ChangeReason = ('$changeReason'),
                 ChangeImpact = ('$changeImpact'),
                        RelBy = ('$updatedBy'),
                      RelDate = ('$updated')
                WHERE    ICAO = ('$icao')
                AND RelStatus = ('$releasedStatus')";
            }
            else 
            {
                //need to log here that the elevation in parsed data is lower than apex so not updated
                $txt = "ICAO: " . $icao . " - Parsed Elevation is lower than apex so elevation is not changed\n";
                fwrite(Self::$redFlagsLog, $txt);
                
                //sql update statment
                $sql = "UPDATE Airport 
                SET  Latitude = ('$latitude'), 
                    Longitude = ('$longitude'),
                MagnVariation = ('$magnvariation'),
                    Aerodrome = ('$airportName'),
                      Updated = ('$updated'),
                    UpdatedBy = ('$updatedBy'),
                 ChangeReason = ('$changeReason'),
                 ChangeImpact = ('$changeImpact'),
                        RelBy = ('$updatedBy'),
                      RelDate = ('$updated')
                WHERE    ICAO = ('$icao')
                AND RelStatus = ('$releasedStatus')";
            }

            if (mysqli_query($conn, $sql)) 
            {
                echo "Airport Record updated successfully\n";
                $txt = "ICAO: " . $icao . "  Changed Field(s): " . $fieldComparison . "  Table: Airport table\n";

                // ***TODO add previous value and new value for changed field
                fwrite(Self::$recordLog, $txt);
            } 
            else
            {
                echo "Error updating airport record: " . mysqli_error($conn) . "\n";
                //log any error
                $error_message = "ICAO: " . $icao . " - Error updating airport record: " . mysqli_error($conn);
                fwrite(Self::$errorLog, $error_message);
                exit;
            }
        }
        function updateDatabase($ICAO)
        {
            $icao = $this->getICAO();
            echo "ICAO: " . $icao . " - Currently being processed ..." . "<br>\n";
            if (array_key_exists($icao, $ICAO))
            {
                //only update for standard data
                if ($this->getDataType() == "S")
                {
                    //execute updateAirport if airport csv file
                    $this->updateAirport($ICAO[$icao]);
                }
            }
            else
            {
                $txt = "ICAO: " . $icao . " - Does not exist in apeX \n";
                echo $txt;
                fwrite(Self::$redFlagsLog, $txt);
            }
        }
    }
    //Runway records
    class Runway extends Record
    {
        //positions defined to contain corresponding information
        const pos_RunwayID = 4;
        const pos_RunwayLength = 5;
        const pos_MagneticBearing = 6;
        const pos_Latitude = 7;
        const pos_Longitude = 8;
        const pos_DisplacedThreshold = 11;
        const pos_RunwayWidth = 13;
        const pos_TrueHeading = 22;
        const pos_LDA = 44;
        const pos_TORA = 46;
        const pos_TODA = 48;
        const pos_ASDA = 50;
        const pos_Cycle = 21;

        //used to keep records in unreleased state
        const unreleasedRecord = "P";
        const tempKey = "TMP";
        const intersectionsLocation = "Intersections";
        const obstaclesLocation = "Obstacles";
        const engineFailureLocation = "EngineFailure";
        const engineFailureObstacleLocation = "EngineFailureObstacles";

        const releasedRunwayStatus = "Y";
        const intersectionStatus = "Y";

        function __construct($rwyRecord)
        {
            parent::__construct($rwyRecord);
        }
        function getRunwayID()
        {
            //take out "RW" from parsed field
            $runwayid = $this->getField(Self::pos_RunwayID);
            $search = "RW";
    
            $trimmed = str_replace($search, "", $runwayid);
            return $trimmed;
        }
        function getAiracRunway($runway)
        {
            $airacRunway = $runway . "-A";
            return $airacRunway;
        }
        function getMagneticBearing()
        {
            //store parsed field
            $original = $this->getField(Self::pos_MagneticBearing);

            if (is_numeric(substr($original, 1)) OR $original == "0")
            {
                //concatanate
                $result = $original/10;
                return $result;
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse MagneticBearing invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse MagneticBearing invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getLatitude()
        {
            //store field
            $original = $this->getField(Self::pos_Latitude);
            if (is_numeric(substr($original, 1)))
            {
                $latitude = round(Self::DMSLatitude($original), 9);
                
                //match with apex for 10 decimal spaces
                return number_format($latitude, 10);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse Latitude invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse Latitude invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getLongitude()
        {
            //store field
            $original = $this->getField(Self::pos_Longitude);
            if (is_numeric(substr($original, 1)))
            {
                $longitude = round(Self::DMSLongitude($original), 9);
        
                //match with apex for 10 decimal spaces
                return number_format($longitude, 10);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse Longitude invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse Longitude invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getDisplacedThreshold()
        {
            return $this->getField(Self::pos_DisplacedThreshold);
        }
        function getRunwayWidth()
        {
            $original = $this->getField(Self::pos_RunwayWidth);
            if (is_numeric($original) OR $original == "0")
            {
                $width = $this->feetToMeters($original);
                
                return doubleval($width);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse RunwayWidth invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse RunwayWidth invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getTrueHeading()
        {
            //store parsed field
            $original = $this->getField(Self::pos_TrueHeading);

            if (is_numeric(substr($original, 1)) OR $original == "0")
            {
                $result = $original/100;
                $result = number_format($result, 10);
                return $result;
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TrueHeading invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TrueHeading invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getLDA()
        {
            $original = $this->getField(Self::pos_LDA);
            if (is_numeric($original) OR $original == "0")
            {
                $lda = $this->feetToMeters($original);
                return doubleval($lda);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse LDA invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse LDA invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getTORA()
        {
            $original = $this->getField(Self::pos_TORA);
            if (is_numeric($original) OR $original == "0")
            {
                $tora = $this->feetToMeters($original);
                return doubleval($tora);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TORA invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TORA invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getTODA()
        {
            $originalTODA = $this->getField(Self::pos_TODA);
            $originalTORA = $this->getField(Self::pos_TORA);
            if (is_numeric($originalTODA) AND $originalTODA >= $originalTORA)
            {
                $toda = $this->feetToMeters($originalTODA);
                return doubleval($toda);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TODA invalid input" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse TODA invalid input\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getASDA()
        {
            $original = $this->getField(Self::pos_ASDA);
            if (is_numeric($original) OR $original == "0")
            {
                $asda = $this->feetToMeters($original);
                return doubleval($asda);
            }
            else
            {
                echo "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse ASDA invalid input(not numeric)" . "<br>\n";
                $error_message = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . " - Could not parse ASDA invalid input(not numeric)\n";
                fwrite(Self::$errorLog, $error_message);
            }
            return NULL;
        }
        function getCycle()
        {
            return intval($this->getField(Self::pos_Cycle));
        }
        function compareMainRunwayFields($ICAO)
        {
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();

            //set array with neeeded data to be compared
            $parsedData = array("MagnHeading" => $this->getMagneticBearing(), "StartLatitude" => $this->getLatitude(),
            "StartLongitude" => $this->getLongitude(), "RunwayWidth" => $this->getRunwayWidth(), "TrueHeading" => $this->getTrueHeading(),
            "TORA" => $this->getTORA(), "TODA" => $this->getTODA(), "LDA" => $this->getLDA(), "ASDA" => $this->getASDA());
            $error = false;
            foreach ($parsedData as $field => $value)
            {
                if ($value == NULL)
                {
                    $txt = "ICAO: " . $this->getICAO() . "  RunwayID: " . $this->getRunwayID() . "  Table: MainRunway" . 
                    "  Field: " . $field . " - Invalid input at this field, update for this record skipped\n";
                    fwrite(Self::$redFlagsLog, $txt);
                    $error = true;
                }
            }
            if ($error == true)
            {
                return Self::invalidRecord;
            }
            $result = array_diff($parsedData, $ICAO);
            if (empty($result)) 
            {
                // list is empty. There are no differences
                return Self::unchangedRecord;
            }
            $keys = array_keys($result);

            //create a string with fields that were changed to be returned
            $updatedFields = implode(", ", $keys);

            return $updatedFields;
        }
        function checkDisplacedThreshold()
        {
            //check parsed displaced threshold
            if ($this->getDisplacedThreshold() == 0)
            {
                return True;
            }
            else
            {
                return False;
            }
        }
        function createAiracRunway($ICAO)
        {        
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $airacRunway = $this->getAiracRunway($runway);
            $RelStatus = Self::unreleasedRecord;
            $updated = date("Y-m-d H:i:s");
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $releasedRunwayStatus = Self::releasedRunwayStatus;

            //sql statement to clone the released copy of runway with changes
            $sql = "INSERT INTO Runway (ICAO, Runway, RelStatus, RelBy, UpdatedBy, Updated, ChangeReason, ChangeImpact, Comments,
            Intersection, TORA, Slope, Elevation, Latitude, Longitude, AlignType, ValidFrom, ValidTo, EffectiveFrom,
            MainRunway, MainRunwayRelStatus, SpecialCare, RelatedRunway, RelatedRelStatus, AiracRunway, TempRunwayInfo)
            SELECT ICAO, ('$airacRunway'), ('$RelStatus'), RelBy, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments,
            Intersection, TORA, Slope, Elevation, Latitude, Longitude, AlignType, ValidFrom, ValidTo, EffectiveFrom,
            ('$airacRunway'), ('$RelStatus'), SpecialCare, ('$runway'), RelatedRelStatus, AiracRunway, TempRunwayInfo
            FROM Runway
            WHERE ICAO = ('$icao') AND Runway = ('$runway') AND RelStatus = ('$releasedRunwayStatus')";

            if (mysqli_query($conn, $sql)) 
            {
                echo "ICAO: " . $icao . "  RunwayID: " . $runway . " - Runway Record cloned successfully\n";
                $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . " - Runway Record cloned successfully\n";
                fwrite(Self::$recordLog, $txt);
            } 
            else
            {
                echo "Error updating Runway record: " . mysqli_error($conn) . "\n";
                //log any error
                $error_message = "ICAO: " . $icao . "  RunwayID: " . $runway . " - Error cloning Runway record: " . mysqli_error($conn);
                fwrite(Self::$errorLog, $error_message);
                exit;
            }
        }
        // When Runway and MainRunway column is equal in Runway Table
        function createAiracMainRunway($ICAO)
        {         
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $magneticBearing = $this->getMagneticBearing();
            $latitude = $this->getLatitude();
            $longitude = $this->getLongitude();
            $runwayWidth = $this->getRunwayWidth();
            $trueHeading = $this->getTrueHeading();
            $tora = $this->getTORA();
            $toda = $this->getTODA();
            $lda = $this->getLDA();
            $asda = $this->getASDA();
            $airacRunway = $this->getAiracRunway($runway);
            $RelStatus = Self::unreleasedRecord;
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            $updated = date("Y-m-d H:i:s");

            //store return value for field comparison
            $fieldComparison = $this->compareMainRunwayFields($ICAO);

            //check for up to date runways
            if ($fieldComparison == Self::unchangedRecord)
            {
                //need to log here that there is no update required for this record
                $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  Table: MainRunway table" . " - This main runway is already up to date according to parsed data\n";
                fwrite(Self::$redFlagsLog, $txt);
                return;
            }
            else if ($fieldComparison == Self::invalidRecord)
            {
                return Self::invalidRecord;
            }

            //check for displaced threshold = 0
            if ($this->checkDisplacedThreshold() == True)
            {
                //sql statement to clone the released copy of runway with changes
                $sql = "INSERT INTO MainRunway (ICAO, MainRunway, RelStatus, RelBy, RelDate, UpdatedBy, Updated, ChangeReason,
                ChangeImpact, Comments, AirportRelStatus, MagnHeading, TrueHeading, Slope, TORA, TODA, LDA, ASDA, StrengthInfo,
                SurfaceType, Grooved, AlignType, MissedApproachCG, StartLatitude, StartLongitude, EndLatitude, EndLongitude,
                StartElevation, EndElevation, RunwayWidth, ValidFrom, ValidTo, EffectiveFrom, SpecialCare, GS, GSSlope, RelatedRunway,
                RelatedRelStatus, AiracRunway, TempRunwayInfo, GroovedPFCSurfaceTO, GroovedPFCSurfaceStopway, GroovedPFCSurfaceLD)
                SELECT ICAO, ('$airacRunway'), ('$RelStatus'), RelBy, RelDate, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments,
                AirportRelStatus, ('$magneticBearing'), ('$trueHeading'), Slope, ('$tora'), ('$toda'), ('$lda'), ('$asda'), StrengthInfo,
                SurfaceType, Grooved, AlignType, MissedApproachCG, ('$latitude'), ('$longitude'), EndLatitude, EndLongitude, StartElevation,
                EndElevation, ('$runwayWidth'), ValidFrom, ValidTo, EffectiveFrom, SpecialCare, GS, GSSlope, ('$runway'),
                ('$releasedRunwayStatus'), AiracRunway,TempRunwayInfo, GroovedPFCSurfaceTO, GroovedPFCSurfaceStopway, GroovedPFCSurfaceLD
                FROM MainRunway
                WHERE ICAO = ('$icao') AND MainRunway = ('$runway') AND RelStatus = ('$releasedRunwayStatus')";
            }
            else
            {
                // dont update longitude and latitude displaced threshold not 0
                $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . " - Displaced Threshold not 0 so Latitude and Longitude not changed in MainRunway table\n";
                fwrite(Self::$redFlagsLog, $txt);
                
                //sql statement to clone the released copy of runway with changes
                $sql = "INSERT INTO MainRunway (ICAO, MainRunway, RelStatus, RelBy, RelDate, UpdatedBy, Updated, ChangeReason,
                ChangeImpact, Comments, AirportRelStatus, MagnHeading, TrueHeading, Slope, TORA, TODA, LDA, ASDA, StrengthInfo,
                SurfaceType, Grooved, AlignType, MissedApproachCG, StartLatitude, StartLongitude, EndLatitude, EndLongitude,
                StartElevation, EndElevation, RunwayWidth, ValidFrom, ValidTo, EffectiveFrom, SpecialCare, GS, GSSlope, RelatedRunway,
                RelatedRelStatus, AiracRunway, TempRunwayInfo, GroovedPFCSurfaceTO, GroovedPFCSurfaceStopway, GroovedPFCSurfaceLD)
                SELECT ICAO, ('$airacRunway'), ('$RelStatus'), RelBy, RelDate, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments,
                AirportRelStatus, ('$magneticBearing'), ('$trueHeading'), Slope, ('$tora'), ('$toda'), ('$lda'), ('$asda'), StrengthInfo,
                SurfaceType, Grooved, AlignType, MissedApproachCG, StartLatitude, StartLongitude, EndLatitude, EndLongitude, StartElevation,
                EndElevation, ('$runwayWidth'), ValidFrom, ValidTo, EffectiveFrom, SpecialCare, GS, GSSlope, ('$runway'),
                ('$releasedRunwayStatus'), AiracRunway,TempRunwayInfo, GroovedPFCSurfaceTO, GroovedPFCSurfaceStopway, GroovedPFCSurfaceLD
                FROM MainRunway
                WHERE ICAO = ('$icao') AND MainRunway = ('$runway') AND RelStatus = ('$releasedRunwayStatus')";
            }
    
            if (mysqli_query($conn, $sql)) 
            {
                echo "ICAO: " . $icao . "  RunwayID: " . $runway . " - MainRunway Record updated successfully \n";
                $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  Changed Field(s): " . $fieldComparison ."  Table: MainRunway table\n";
                fwrite(Self::$recordLog, $txt);
            } 
            else
            {
                echo "Error updating MainRunway record: " . mysqli_error($conn) . "\n";
                $error_message = "ICAO: " . $icao . "  Runway: " . $runway . " - Error updating MainRunway record: " . mysqli_error($conn);
                fwrite(Self::$errorLog, $error_message);
                exit;
            }   
        }
        static function intersectionCheck($ICAO, $ICAO_Intersection)
        {
            $conn = Self::$connectionString;
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            $intersection = Self::intersectionStatus;
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);
            $sql="SELECT * FROM Runway WHERE ICAO in ('$ICAO_string') AND Intersection = ('$intersection') AND RelStatus = ('$releasedRunwayStatus')";
            $result = mysqli_query($conn, $sql);

            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                while($row = mysqli_fetch_assoc($result)) 
                {  
                    $icao = $row["ICAO"];
                    $runway = $row["MainRunway"];
                    $intersection = $row["Runway"];
                    
                    //if current parsed icao matches apex and not a temporary runway
                    if (strpos($runway, Self::tempKey) == false AND $row["ValidFrom"] == NULL)
                    {
                        //set up structure of array with intial values
                        if (array_key_exists($icao, $ICAO_Intersection))
                        {
                            if (array_key_exists(Self::intersectionsLocation, $ICAO_Intersection[$icao]))
                            {
                                if (array_key_exists($runway, $ICAO_Intersection[$icao][Self::intersectionsLocation]))
                                {
                                    array_push($ICAO_Intersection[$icao][Self::intersectionsLocation][$runway], $intersection);
                                }
                                else
                                {
                                    $ICAO_Intersection[$icao][Self::intersectionsLocation][$runway] = array($intersection);
                                }
                            }
                            else
                            {
                                $ICAO_Intersection[$icao][Self::intersectionsLocation][$runway] = array($intersection);
                            }
                        }
                    }
                }
            }
            return $ICAO_Intersection;
        }
        static function obstacleCheck($ICAO, $ICAO_Intersection)
        {
            $conn = Self::$connectionString;
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);
            $sql="SELECT * FROM Obstacle WHERE ICAO in ('$ICAO_string') AND MainRunwayRelStatus = ('$releasedRunwayStatus')";
            $result = mysqli_query($conn, $sql);

            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                while($row = mysqli_fetch_assoc($result)) 
                {  
                    $icao = $row["ICAO"];
                    $runway = $row["MainRunway"];
                    //set up structure of array with intial values
                    if (array_key_exists($icao, $ICAO_Intersection))
                    {
                        if (array_key_exists(Self::obstaclesLocation, $ICAO_Intersection[$icao]))
                        {
                            if (array_key_exists($runway, $ICAO_Intersection[$icao][Self::obstaclesLocation]))
                            {
                                array_push($ICAO_Intersection[$icao][Self::obstaclesLocation][$runway], $row["Sequence"]);
                            }
                            else
                            {
                                $ICAO_Intersection[$icao][Self::obstaclesLocation][$runway] = array($row["Sequence"]);
                            }
                        }
                        else
                        {
                            $ICAO_Intersection[$icao][Self::obstaclesLocation][$runway] = array($row["Sequence"]);
                        }
                    }
                }
            }
            return $ICAO_Intersection;
        }
        static function engineFailureCheck($ICAO, $ICAO_Intersection)
        {
            $conn = Self::$connectionString;
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);
            $sql="SELECT * FROM EngineFailure WHERE ICAO in ('$ICAO_string') AND MainRunwayRelStatus = ('$releasedRunwayStatus')";
            $result = mysqli_query($conn, $sql);

            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                while($row = mysqli_fetch_assoc($result)) 
                {  
                    //loop through all icaos
                    for ($i = 0; $i < count($ICAO); $i++)  
                    {
                        $icao = $row["ICAO"];
                        $runway = $row["MainRunway"];
                        
                        //if current parsed icao matches apex
                        if ($ICAO[$i] == $icao)
                        {
                            //set up structure of array with intial values
                            if (array_key_exists($icao, $ICAO_Intersection))
                            {
                                if (array_key_exists(Self::engineFailureLocation, $ICAO_Intersection[$icao]))
                                {
                                    if (array_key_exists($runway, $ICAO_Intersection[$icao][Self::engineFailureLocation]))
                                    {
                                        array_push($ICAO_Intersection[$icao][Self::engineFailureLocation][$runway], $row["SplayType"]);
                                    }
                                    else
                                    {
                                        $ICAO_Intersection[$icao][Self::engineFailureLocation][$runway] = array($row["SplayType"]);
                                    }
                                }
                                else
                                {
                                    $ICAO_Intersection[$icao][Self::engineFailureLocation][$runway] = array($row["SplayType"]);
                                }
                            }
                        }
                    }
                }
            }
            return $ICAO_Intersection;
        }
        static function engineFailureObstacleCheck($ICAO, $ICAO_Intersection)
        {
            $conn = Self::$connectionString;
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            //seperate icao array into comma delimited string
            $ICAO_string = implode("', '", $ICAO);
            $sql="SELECT * FROM EngineFailureObstacle WHERE ICAO in ('$ICAO_string') AND MainRunwayRelStatus = ('$releasedRunwayStatus')";
            $result = mysqli_query($conn, $sql);

            //if rows are selected
            if (mysqli_num_rows($result) > 0) 
            {
                while($row = mysqli_fetch_assoc($result)) 
                {  
                    //loop through all icaos
                    for ($i = 0; $i < count($ICAO); $i++)  
                    {
                        $icao = $row["ICAO"];
                        $runway = $row["MainRunway"];
                        
                        //if current parsed icao matches apex
                        if ($ICAO[$i] == $icao)
                        {
                            //set up structure of array with intial values
                            if (array_key_exists($icao, $ICAO_Intersection))
                            {
                                $splayType = $row["SplayType"];
                                if (array_key_exists(Self::engineFailureObstacleLocation, $ICAO_Intersection[$icao]))
                                {
                                    if (array_key_exists($runway, $ICAO_Intersection[$icao][Self::engineFailureObstacleLocation]))
                                    {
                                        array_push($ICAO_Intersection[$icao][Self::engineFailureObstacleLocation][$runway], array("SplayType" => $splayType, "ObsSequence" => $row["ObsSequence"]));
                                    }
                                    else
                                    {
                                        $ICAO_Intersection[$icao][Self::engineFailureObstacleLocation][$runway] = array(array("SplayType" => $row["SplayType"], "ObsSequence" => $row["ObsSequence"]));
                                    }
                                }
                                else
                                {
                                    $ICAO_Intersection[$icao][Self::engineFailureObstacleLocation][$runway] = array(array("SplayType" => $row["SplayType"], "ObsSequence" => $row["ObsSequence"]));
                                }
                            }
                        }
                    }
                }
            }
            return $ICAO_Intersection;
        }
        function createAiracIntersections($ICAO_Intersection)
        {
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $RelStatus = Self::unreleasedRecord;
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $updated = date("Y-m-d H:i:s");
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            
            //check to see if there are any intersections
            if (array_key_exists(Self::intersectionsLocation, $ICAO_Intersection))
            {
                //check if the current runway has intersections
                if (array_key_exists($runway, $ICAO_Intersection[Self::intersectionsLocation]))
                {
                    //loop through each intersection
                    for ($i = 0; $i < count($ICAO_Intersection[Self::intersectionsLocation][$runway]); $i++)
                    {
                        $intersectionName = $ICAO_Intersection[Self::intersectionsLocation][$runway][$i];

                        $airacRunway = $this->getAiracRunway($intersectionName);
                        $airacMainRunway = $this->getAiracRunway($runway);
                        //sql statement to clone the released copy of runway with changes
                        $sql = "INSERT INTO Runway (ICAO, Runway, RelStatus, RelBy, UpdatedBy, Updated, ChangeReason, ChangeImpact, Comments,
                        Intersection, TORA, Slope, Elevation, Latitude, Longitude, AlignType, ValidFrom, ValidTo, EffectiveFrom,
                        MainRunway, MainRunwayRelStatus, SpecialCare, RelatedRunway, RelatedRelStatus, AiracRunway, TempRunwayInfo)
                        SELECT ICAO, ('$airacRunway'), ('$RelStatus'), RelBy, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments,
                        Intersection, TORA, Slope, Elevation, Latitude, Longitude, AlignType, ValidFrom, ValidTo, EffectiveFrom,
                        ('$airacMainRunway'), ('$RelStatus'), SpecialCare, ('$intersectionName'), ('$releasedRunwayStatus'), AiracRunway, TempRunwayInfo
                        FROM Runway
                        WHERE ICAO = ('$icao') AND Runway = ('$intersectionName') AND RelStatus = ('$releasedRunwayStatus')";

                        if (mysqli_query($conn, $sql)) 
                        {
                            $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  Intersection: " . $intersectionName . " - Intersection cloned successfully\n";
                            fwrite(Self::$recordLog, $txt);
                        } 
                        else
                        {
                            echo "Error cloning Intersection record: " . mysqli_error($conn) . "\n";
                            //log any error
                            $error_message = "ICAO: " . $icao . "  RunwayID: " . $runway .  "  Intersection: " . $intersectionName . " - Error cloning Intersection record: " . mysqli_error($conn);
                            fwrite(Self::$errorLog, $error_message);
                            exit;
                        }
                    }
                }
            }
        }
        function createAiracObstacles($ICAO_Intersection)
        {
            //set up variables to be used
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $RelStatus = Self::unreleasedRecord;
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $updated = date("Y-m-d H:i:s");
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            
            if (array_key_exists(Self::obstaclesLocation, $ICAO_Intersection))
            {
                //check if the current runway has obstacles
                if (array_key_exists($runway, $ICAO_Intersection[Self::obstaclesLocation]))
                {
                    //loop through all obstacles for particular runway
                    for ($i = 0; $i < count($ICAO_Intersection[Self::obstaclesLocation][$runway]); $i++)
                    {
                        $obstacleName = $ICAO_Intersection[Self::obstaclesLocation][$runway][$i];
                        $airacMainRunway = $this->getAiracRunway($runway);

                        //sql statement to clone the released copy of obstacle with changes
                        $sql = "INSERT INTO Obstacle (ICAO, MainRunway, MainRunwayRelStatus, Sequence, Height, Altitude, Latitude, Longitude,
                        ObsType, DataSource, Comments, Reference, Distance, Lateral, UpdatedBy, Updated, ChangeReason, ChangeImpact)
                        SELECT ICAO, ('$airacMainRunway'), ('$RelStatus'), Sequence, Height, Altitude, Latitude, Longitude,
                        ObsType, DataSource, Comments, Reference, Distance, Lateral, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact')
                        FROM Obstacle
                        WHERE ICAO = ('$icao') AND MainRunway = ('$runway') AND Sequence = ('$obstacleName') AND MainRunwayRelStatus = ('$releasedRunwayStatus')";

                        if (mysqli_query($conn, $sql)) 
                        {
                            $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  Obstacle: " . $obstacleName . " - Obstacle cloned successfully\n";
                            fwrite(Self::$recordLog, $txt);
                        } 
                        else
                        {
                            echo "Error cloning Obstacle record: " . mysqli_error($conn) . "\n";
                            //log any error
                            $error_message = "ICAO: " . $icao . "  RunwayID: " . $runway .  "  Obstacle: " . $obstacleName . " - Error cloning Obstacle record: " . mysqli_error($conn);
                            fwrite(Self::$errorLog, $error_message);
                            exit;
                        }
                    }
                }
            }
        }
        function createAiracEngineFailure($ICAO_Intersection)
        {
            //set up variables to be used
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $RelStatus = Self::unreleasedRecord;
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $updated = date("Y-m-d H:i:s");
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            
            if (array_key_exists(Self::engineFailureLocation, $ICAO_Intersection))
            {
                //check if the current runway has obstacles
                if (array_key_exists($runway, $ICAO_Intersection[Self::engineFailureLocation]))
                {
                    //loop through all obstacles for particular runway
                    for ($i = 0; $i < count($ICAO_Intersection[Self::engineFailureLocation][$runway]); $i++)
                    {
                        $splayType = $ICAO_Intersection[Self::engineFailureLocation][$runway][$i];
                        $airacMainRunway = $this->getAiracRunway($runway);

                        //sql statement to clone the released copy of obstacle with changes
                        $sql = "INSERT INTO EngineFailure (ICAO, MainRunway, MainRunwayRelStatus, SplayType, Priority, Active, WptIdentifier, WptCountryCode, 
                        WptType, Description, Calculations, Reference, ValidFor, UpdatedBy, Updated, ChangeReason, ChangeImpact, Comments, Flaps, Turn,
                        BankAngle, MinV2, MaxV2, Note, TakeoffThrust, ProcDistance, ProcAltitude, ProcComments, TrimmedDescription, OriginalSplayType)
                        SELECT ICAO, ('$airacMainRunway'), ('$RelStatus'), SplayType, Priority, Active, WptIdentifier, WptCountryCode, 
                        WptType, Description, Calculations, Reference, ValidFor, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments, Flaps,
                        Turn, BankAngle, MinV2, MaxV2, Note, TakeoffThrust, ProcDistance, ProcAltitude, ProcComments, TrimmedDescription, OriginalSplayType
                        FROM EngineFailure
                        WHERE ICAO = ('$icao') AND MainRunway = ('$runway') AND SplayType = ('$splayType') AND MainRunwayRelStatus = ('$releasedRunwayStatus')";

                        if (mysqli_query($conn, $sql)) 
                        {
                            $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  EngineFailure: " . $splayType . " - EngineFailure cloned successfully\n";
                            fwrite(Self::$recordLog, $txt);
                        } 
                        else
                        {
                            echo "Error cloning EngineFailure record: " . mysqli_error($conn) . "\n";
                            //log any error
                            $error_message = "ICAO: " . $icao . "  RunwayID: " . $runway .  "  EngineFailure: " . $splayType . " - Error cloning EngineFailure record: " . mysqli_error($conn);
                            fwrite(Self::$errorLog, $error_message);
                            exit;
                        }
                    }
                }
            }
        }
        function createAiracEngineFailureObstacles($ICAO_Intersection)
        {
            //set up variables to be used
            $conn = Self::$connectionString;
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $RelStatus = Self::unreleasedRecord;
            $updatedBy = Self::updatedBy;
            $changeReason = Self::changeReason;
            $changeImpact = Self::changeImpact;
            $updated = date("Y-m-d H:i:s");
            $releasedRunwayStatus = Self::releasedRunwayStatus;
            
            if (array_key_exists(Self::engineFailureObstacleLocation, $ICAO_Intersection))
            {
                //check if the current runway has obstacles
                if (array_key_exists($runway, $ICAO_Intersection[Self::engineFailureObstacleLocation]))
                {
                    //loop through all obstacles for particular runway
                    for ($i = 0; $i < count($ICAO_Intersection[Self::engineFailureObstacleLocation][$runway]); $i++)
                    {
                        $ObsSequence = $ICAO_Intersection[Self::engineFailureObstacleLocation][$runway][$i]["ObsSequence"];
                        $splayType = $ICAO_Intersection[Self::engineFailureObstacleLocation][$runway][$i]["SplayType"];
                        $airacMainRunway = $this->getAiracRunway($runway);

                        //sql statement to clone the released copy of obstacle with changes
                        $sql = "INSERT INTO EngineFailureObstacle (ICAO, MainRunway, MainRunwayRelStatus, SplayType, Priority,
                        ObsSequence, Altitude, Height, Distance, Lateral, UpdatedBy, Updated, ChangeReason, ChangeImpact, Comments)
                        SELECT ICAO, ('$airacMainRunway'), ('$RelStatus'), SplayType, Priority, ObsSequence, Altitude, Height,
                        Distance, Lateral, ('$updatedBy'), ('$updated'), ('$changeReason'), ('$changeImpact'), Comments
                        FROM EngineFailureObstacle
                        WHERE ICAO = ('$icao') AND MainRunway = ('$runway') AND ObsSequence = ('$ObsSequence') 
                        AND MainRunwayRelStatus = ('$releasedRunwayStatus') AND SplayType = ('$splayType')";

                        if (mysqli_query($conn, $sql)) 
                        {
                            $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . "  EngineFailureObstacle: " . $ObsSequence . " - EngineFailureObstacle cloned successfully\n";
                            fwrite(Self::$recordLog, $txt);
                        } 
                        else
                        {
                            echo "Error cloning EngineFailureObstacle record: " . mysqli_error($conn) . "\n";
                            //log any error
                            $error_message = "ICAO: " . $icao . "  RunwayID: " . $runway .  "  EngineFailureObstacle: " . $ObsSequence . " - Error cloning EngineFailureObstacle record: " . mysqli_error($conn);
                            fwrite(Self::$errorLog, $error_message);
                            exit;
                        }
                    }
                }
            }
        }
        function updateDatabase($ICAO)
        {
            $icao = $this->getICAO();
            $runway = $this->getRunwayID();
            $airacRunway = $this->getAiracRunway($runway);

            //ensure that a released record exists TODO: check for release status field ******************
            if (array_key_exists($icao, $ICAO))
            {
                if (array_key_exists($runway, $ICAO[$icao]))
                {
                    //ensure an airac runway doesnt already exist
                    if (!array_key_exists($airacRunway, $ICAO[$icao]))
                    {
                        echo "ICAO: " . $icao . "  RunwayID: " . $runway . " - Currently being processed ..." . "<br>\n";

                        //only update for standard data
                        if ($this->getDataType() == "S")
                        {
                            //update MainRunway
                            
                            if ($this->createAiracMainRunway($ICAO[$icao][$runway]) == Self::invalidRecord)
                            {
                                return;
                            }
                            $this->createAiracRunway($ICAO[$icao][$runway]);
                            $this->createAiracIntersections($ICAO[$icao]);
                            $this->createAiracObstacles($ICAO[$icao]);
                            $this->createAiracEngineFailure($ICAO[$icao]);
                            $this->createAiracEngineFailureObstacles($ICAO[$icao]);
                        }
                    }
                    else
                    {
                        echo "ICAO: " . $icao . "  RunwayID: " . $runway . " - There is alrady an airac record for this runway, skipped update\n" . "<br>";
                        //skip record and log that an airac record already exists
                        $txt = "ICAO: " . $icao . "  RunwayID: " . $runway . " - There is alrady an airac record for this runway, skipped update\n";
                        fwrite(Self::$redFlagsLog, $txt);
                    }
                }
            }
        }
    }
    //check command line input arguments to ensure only one argument passed
    if (isset($argc) == False or $argc != 2) 
    {
        die("Invalid Input");
    }

    echo "Generating files...\n";

    //concatnate the full file path
    $filePath = $fileFolderLocation . $argv[1];
    
    //open file
    ini_set('auto_detect_line_endings',TRUE);  
    
    $myfile = fopen($filePath, "r") or die("Unable to open file!");

    //setup log files
    $filePath = $fileFolderLocation . $GLOBALS['errorLogFile'];
    Record::$errorLog = fopen($filePath, "w");
    if(!Record::$errorLog)
    {
        echo "An error opening error.log file occured\n";
    }

    $filePath = $fileFolderLocation . $GLOBALS['recordsLogFile'];
    Record::$recordLog = fopen($filePath, "w");
    if(!Record::$recordLog)
    {
        echo "An error opening records.log file occured\n";
    }

    $filePath = $fileFolderLocation . $GLOBALS['redFlagsLogFile'];
    Record::$redFlagsLog = fopen($filePath, "w");
    if(!Record::$redFlagsLog)
    {
        echo "An error opening redFlags.log file occured\n";
    }
    
    //intialize arrays
    $records = array();
    $ICAO = array();

    //Add each runway detected in the file to the array of records
    while(($record = fgetcsv($myfile,0,",")) !== FALSE)
    {
        if( Record::isAirportOrRunwayRecord($record) == Record::isRunway)
        {
            $rwy = new Runway($record);
            $records[] = $rwy;

            // to ensure repeated entries are not added as there are 
            // multiple runways with same icao
            if (!in_array($rwy->getICAO(), $ICAO))
            {
                $ICAO[] = $rwy->getICAO(); 
            }
        }
        else if ( Record::isAirportOrRunwayRecord($record) == Record::isAirport)
        {
            $air = new Airport($record);
            $records[] = $air;
            
            $ICAO[] = $air->getICAO(); 
        }
        else if ( Record::isAirportOrRunwayRecord($record) == Record::isHeader)
        {
            continue;
        }
        else
        {
            //there was an error reading the record and it could not be identified as Airport or Runway
            echo "The script could not identify the record(s) to be an airport or runway record check error log";
            $error_message = "Could not identify current record(s) to be an airport or runway record check format of csv file";
            fwrite(Record::$errorLog, $error_message);
        }
    }

    if (Record::$isRunway == True)
    {
        fwrite(Record::$recordLog, "Runway Records Affected:\n");
    }
    else
    {
        fwrite(Record::$recordLog, "Airport Records Affected:\n");
    }

    //establish a connection to database and save connection string
    Record::connectDatabase();
    
    if ($GLOBALS['mode'] == Record::debugMode)
    {
        Record::databaseDump($ICAO, Record::beforeDiff);
    }

    if (Record::$isRunway == True)
    {
        //find the intersection between parsed icaos and database by querying the database
        $ICAO_Intersection = Record::queryData($ICAO);
        $ICAO_Intersection = Runway::intersectionCheck($ICAO, $ICAO_Intersection);
        $ICAO_Intersection = Runway::obstacleCheck($ICAO, $ICAO_Intersection);
        $ICAO_Intersection = Runway::engineFailureCheck($ICAO, $ICAO_Intersection);
        $ICAO_Intersection = Runway::engineFailureObstacleCheck($ICAO, $ICAO_Intersection);
    }
    else
    {
        //find the intersection between parsed icaos and database by querying the database
        $ICAO_Intersection = Record::queryData($ICAO);
    }

    //loop through each record in csv file
    foreach ($records as $rec)
    {
        //update the database
        $rec->updateDatabase($ICAO_Intersection);
    }

    if ($GLOBALS['mode'] == Record::debugMode)
    {
        Record::databaseDump($ICAO, Record::afterDiff);
        Record::databaseDiff();
        Record::databaseDiffFromLastRun();
        fclose(Record::$DatabaseDumpLog_before);
        fclose(Record::$DatabaseDumpLog_after);
        fclose(Record::$DatabaseDiffLog);
        fclose(Record::$DatabaseDiffChangesLog);
    }

    // Close DB MySQL
    mysqli_close(Record::$connectionString);

    fclose(Record::$errorLog);
    fclose(Record::$recordLog);
    fclose(Record::$redFlagsLog);


    echo "Done Excution";
    fclose($myfile);
?>