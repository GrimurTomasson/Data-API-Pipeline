switch -regex -file .\dapi.env {
	'^DAPI_' {
		$arr = $_.Split("=")
		[Environment]::SetEnvironmentVariable($arr[0], $arr[1])
	}
}