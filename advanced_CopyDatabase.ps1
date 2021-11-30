param
(
    [Parameter(Mandatory = $false)] [string]$SourceServerName, # SQL Server name where the database should be restored to.
    [Parameter(Mandatory = $false)] [string]$DestServerName, # If parameter is included, the $toDatabaseName will be placed in the same Availability Group (if any) as the $fromDatabaseName (unless the $toDatabaseName is already in an Availability Group or $useExistingAGName is used).
    [Parameter(Mandatory = $false)] [string]$SourceDatabaseName, # Name of the database that will be created. Be careful, as this database will automatically be dropped if it exists.
    [Parameter(Mandatory = $false)] [string]$DestDatabaseName, # Name of the database that will be created. Be careful, as this database will automatically be dropped if it exists.
    [Parameter(Mandatory = $false)] [bool]$RetainSecurity, # Retain same security as in destination
    [Parameter(Mandatory = $false)] [string]$UseExistingAGName # Add the destination database to the existing AG group
)

#$SourceServerName='LGC-DEVC01-02\SS02'
#$SourceDatabaseName='testdb1'
#$DestServerName='LGC-DEVC01-02\SS02'
#$DestServerName='EUISQL04'
#$DestDatabaseName = 'testdb3'
#$RetainSecurity = 1
#$UseExistingAGName = 'yes'


#Location where backup files are stored temporarily
$share = '\\access.blob.ent.sos.eu\sqlbackup$'

function ExtractSecurity {
    param ([string]$DestServerName , [string]$DestDatabase)
    #Extract users from source database and save them for further processing
    write-host "Saving users from "  $DestDatabase " on " $DestServerName " for later usage"
    $DestinationSqlConnection = [System.Data.SqlClient.SqlConnection]::new()
    #$ConnectionString = "server='$($DestServerName)';database='$($DestDatabase)';trusted_connection=yes;Application Name='Netic_CopyDB'"
        
    $DestinationSqlConnection.ConnectionString = "server=$($DestServerName);database=$($DestDatabase);trusted_connection=yes;Application Name=Netic_CopyDB"
    $DestinationSqlCommand = $DestinationSqlConnection.CreateCommand()

    #Command to extract users
    $DestinationSqlCommand.CommandText = @"
        SELECT 'CREATE USER [' + name + '] for login [' + name + '] with default_schema=' + isnull(default_schema_name,'dbo')
        from sys.database_principals
        where Type in ('U','G')
        and name <> 'dbo'
        union all
        SELECT 'CREATE ROLE [' + name + ']'
        FROM sys.database_principals
        where type='R'
        and is_fixed_role = 0
        and name <> 'public'
        union all
        SELECT 'EXECUTE sp_AddRoleMember ''' + roles.name + ''', ''' + users.name + ''''
        from sys.database_principals users
        inner join sys.database_role_members link
        on link.member_principal_id = users.principal_id
        inner join sys.database_principals roles
        on roles.principal_id = link.role_principal_id
        where users.name not in ('dbo')
"@

    $dataAdapter = [System.Data.SqlClient.SqlDataAdapter]::new($DestinationSqlCommand)
    $DestinationDataSet = [System.Data.DataSet]::new()
    $null = $DataAdapter.Fill($DestinationDataSet)
        
    return , $DestinationDataset.Tables[0]
}

	
function  AddSecurityToDestination {
    #Add users from ExtractSecurity to the database		
    param ([string]$DestServerName , [string]$DestDatabase, [system.data.datatable]$Table)

    if ($Table) {		
        write-host "Adding " $Table.Rows.Count " user(s) to " $DestDatabase
        if ($Table.Rows.Count -gt 0) {
            try {
                foreach ($Row in $Table.Rows) {
                    Invoke-DbaQuery -Sqlinstance $DestServerName -Database $DestDatabase -Query $($Row[0])
                }
            }
            catch {
                $message = $_
                write-warning $message
            }
        }
    }
}


function CopyDatabase {
    param([string]$SourceServerName, [string]$DestServerName, [string]$SourceDatabase, [string]$DestDatabase, [bool]$RetainSecurity, [string]$AvailabilityGroup)
                
    if ($RetainSecurity) {
        #Connection to source database to extract logins to be delete
        $SourceSqlConnection = [System.Data.SqlClient.SqlConnection]::new()
        #$ConnectionString = "server='$($SourceServerName)';database='$($SourceDatabase)';trusted_connection=yes;Application Name='Netic_CopyDB'"

        $SourceSqlConnection.ConnectionString = "server=$($SourceServerName);database=$($SourceDatabase);trusted_connection=yes;Application Name=Netic_CopyDB"
        $SourceSqlCommand = $SourceSqlConnection.CreateCommand()
            
        $SourceSqlCommand.CommandText = "select 'drop user [' + name + ']' from sys.database_principals where type in ('S','U','G') and principal_id > 4"

        try {
            $DropDataAdapter = [System.Data.SqlClient.SqlDataAdapter]::new($SourceSqlCommand)
            $SourceDataSet = [System.Data.DataSet]::new()
            $DropDataAdapter.Fill($SourceDataSet)
        }
        catch {
            write-host "No data from source"
        }
        write-host "User to drop from source database:" $SourceDataSet.Tables[0].Rows.Count.ToString()
    }
                    
    #Backup database on source
    write-host "Backup database" $SourceDatabase " on " $SourceServerName " for restore on " $DestServerName
    Backup-DbaDatabase -SqlInstance $SourceServerName -Database $SourceDatabase -Filepath $Share"\"$SourceDatabase".bak" -CopyOnly -WithFormat
        
    #Restore database on destination
    write-host "Restore database" $DestDatabase " on " $DestServerName
    Restore-DbaDatabase -Sqlinstance $DestServerName -DatabaseName $DestDatabase -Path $Share"\"$SourceDatabase".bak" -WithReplace -ReplaceDbNameInFile
    #Cleanup after restore
    write-host "Delete backup file on " $Share
    try {
        Remove-Item -Path $Share"\"$SourceDatabase".bak"
    }
    catch {
        $message = $_
        write-host $message
    }
        
    #If security should be perserved add the users again.
      
    if ($RetainSecurity) { 
        write-host "Remove obselete users from destination after restore"
        #Remove users from source database
        if ($SourceDataset.Tables[0].Rows.Count -gt 0) {
            try {
                foreach ($SourceRow in $SourceDataset.Tables[0].Rows) {
                    Invoke-DbaQuery -Sqlinstance $DestServerName -Database $DestDatabase -Query $($SourceRow[0])
                    write-host $SourceRow[0]
                }
            }
            catch {
                $message = $_
                write-warning $message
            }
                     
        }
    }
       
    #If AvailabilityGroup parameter has been passed add the destination database to the AG
    if ($AvailabilityGroup) {
        write-host "Adding database to AG:" $AvailabilityGroup
        Backup-DbaDatabase -SqlInstance $DestServerName -Database $DestDatabase -Filepath NUL
        Add-DbaAgDatabase -SqlInstance $DestServername -AvailabilityGroup $AvailabilityGroup -Database $DestDatabase -SeedingMode Automatic
    } 
        
}

function CheckDatabase
{ <#
    Checks if a database exists on the server. If the server does not exists script is aborted

    Returns true or false
    #>
    param([string]$ServerName, [string]$Database)

    $remoteSqlConnection = [System.Data.SqlClient.SqlConnection]::new()
    #$ConnectionString = "server='$($ServerName)';database='master';trusted_connection=yes;Application Name='Netic_CopyDB'"
    $remoteSqlConnection.ConnectionString = "server='$($ServerName)';database='master';trusted_connection=yes;Application Name=Netic_CopyDB"
    $remoteSqlCommand = $remoteSqlConnection.CreateCommand()
        
    $sqlquery = "select name from sys.databases where name ='$($Database)'"
    $remoteSqlCommand.CommandText = $sqlquery

    write-host "Checking if database " $Database " exists on " $ServerName
    try {
        $dataAdapter = [System.Data.SqlClient.SqlDataAdapter]::new($remoteSqlCommand)
        $dataAdapter.SelectCommand.CommandTimeout = 10
        $remoteDataSet = [System.Data.DataSet]::new()
        $null = $dataAdapter.Fill($remoteDataSet)
    }
    catch {
        write-host -ForegroundColor red "Server $($Servername)) does not exist"
        exit
    }

    if ($remoteDataSet.Tables[0].Rows.Count -gt 0) {     
        return $true
    } 
    else {
        return $false
    }
        
    $remoteSqlConnection.Dispose()
    $remoteSqlCommand.Dispose()
    $dataAdapter.Dispose()

}



#Check if database exists on source
$SourceExists = CheckDatabase -ServerName  $SourceServerName -Database  $SourceDatabaseName


if ($SourceExists -eq $false) {
    write-host 'Source database does not exists'
    exit
}
else {
    write-host -ForegroundColor Green "Source database " $SourceDatabaseName " exists, starting procedure..."
}

#Check if database exists on destination
$DestExists = CheckDatabase -ServerName  $DestServerName -Database  $DestDatabaseName
        
if ($DestExists) {
    Write-host -ForegroundColor Green "Database $($DestDatabaseName)) exists on destination, database will be overwritten"
}
else {
    Write-Host -ForegroundColor Red "Database does not exsist on destination, new database will be created"
}

$isDestAg = Get-DbaAgDatabase -SqlInstance $DestServername -Database $DestDatabaseName


#Make sure RetainSecurity is set to false if destination database does not exists
if ($DestExists -eq $false) {
    $RetainSecurity = 0
}

if (($RetainSecurity -eq $true) -and ($DestExists -eq $true)) {
    [System.Data.DataTable]$OrgSecurityInfo = ExtractSecurity -DestServerName $DestServerName -DestDatabase $DestDatabaseName
}


if ($isDestAg.AvailabilityGroup) {
    #Get information about availability group on destination
    $DestAvailabilityGroup = Get-DbaAgReplica -SqlInstance $DestServerName | Select-Object AvailabilityGroup | Get-Unique
    if ($DestAvailabilityGroup.AvailabilityGroup) {
        $DestAvailabilityGroupReplicas = Get-DbaAgReplica -SqlInstance $DestServerName -AvailabilityGroup $DestAvailabilityGroup.AvailabilityGroup
        $DestPrimaryReplica = ($DestAvailabilityGroupReplicas | Where-Object role -eq 'Primary').Name
        $DestSecondaryReplica = ($DestAvailabilityGroupReplicas | Where-Object role -eq 'Secondary').Name
        #write-host $DestPrimaryReplica
        #write-host $DestSecondaryReplica
        $DestServerName = $DestPrimaryReplica
    }
}


#Get information about availability group on source
$SourceAvailabilityGroup = Get-DbaAgReplica -SqlInstance $SourceServerName | Select-Object AvailabilityGroup | Get-Unique
if ($SourceAvailabilityGroup.AvailabilityGroup) {
    $SourceAvailabilityGroupReplicas = Get-DbaAgReplica -SqlInstance $SourceServerName -AvailabilityGroup $SourceAvailabilityGroup.AvailabilityGroup
    $SourcePrimaryReplica = ($SourceAvailabilityGroupReplicas | Where-Object role -eq 'Primary').Name
    #$SourceSecondaryReplica = ($SourceAvailabilityGroupReplicas | Where-Object role -eq 'Secondary').Name
    $SourceServerName = $SourcePrimaryReplica
}

#If server is AG, remove the database from AG and delete database on secondary
if (($DestServerName -eq $DestPrimaryReplica) -and ($DestExists -eq $true)) {
    write-host "Removing database from Availability group" $DestAvailabilityGroup.AvailabilityGroup
    Remove-DbaAgDatabase -SqlInstance $DestPrimaryReplica -AvailabilityGroup $DestAvailabilityGroup -Database $DestDatabaseName -Confirm:$false
    Write-host "Removing database from secondary replica" $DestSecondaryReplica
    Remove-DbaDatabase -SqlInstance $DestSecondaryReplica -Database $DestDatabaseName -Confirm:$false
}

#
# Check secondary replica for left overdatabase, delete if exists
#
if (($DestServerName -eq $DestPrimaryReplica) -and ($DestExists -eq $false)) {
    if (CheckDatabase -ServerName $DestSecondaryReplica -Database  $DestDatabaseName -eq $true) {
        write-host "Database still exists on secondary replica... removing it"
        Remove-DbaDatabase -SqlInstance $DestSecondaryReplica -Database $DestDatabaseName -Confirm:$false
    }
}

#Remove database from server if not a AG
if (($DestServerName -ne $DestPrimaryReplica) -and ($DestExists -eq $true)) {
    write-host "Removing database from stand-alone server" 
    Remove-DbaDatabase -SqlInstance $DestServerName -Database $DestDatabaseName -Confirm:$false
}

if ($UseExistingAGName) {
    #Execute Execute Execute
    CopyDatabase -SourceServerName $SourceServerName -DestServerName $DestServerName -SourceDatabase $SourceDatabaseName -DestDatabase $DestDatabaseName -RetainSecurity $RetainSecurity -AvailabilityGroup $UseExistingAGName
    write-host "Copied database and added database to AG"

}
else {
    CopyDatabase -SourceServerName $SourceServerName -DestServerName $DestServerName -SourceDatabase $SourceDatabaseName -DestDatabase $DestDatabaseName -RetainSecurity $RetainSecurity # -AvailabilityGroup $DestAvailabilityGroup.AvailabilityGroup
    write-host "Copied database and added database to AG"

}

if ($RetainSecurity = 1) {
    if ($OrgSecurityInfo) {
        write-host "Adding users to database as last step"
        AddSecurityToDestination -DestServerName $DestServerName -DestDatabase $DestDatabaseName -Table $OrgSecurityInfo
        write-host $LASTEXITCODE
    }
}