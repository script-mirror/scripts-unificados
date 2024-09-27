function Get-AlterarJSON($nameOperator, $scriptPath) {	
	echo "INICIO ALTERACAO JSON - remover $($nameOperator) no path $($scriptPath)"
	$json = "ONS.MPV.ConsoleApplication.deps.json"
	
	$inputFile  = "$($scriptPath)\$($json)"

	$apijson = Get-Content -Path $inputFile -Raw | ConvertFrom-Json

	foreach ($element in $apijson.PSObject.Properties) {
		$objName = $element.Name
		$objValue = $element.Value
		$objProperties = $objValue.PSObject.Properties
		
		#echo "ELEMENT NAME = $($objName)"
		
		foreach ($prop in $objProperties) {
			#echo "PROP NAME = $($prop.Name)"
			
			if($prop.Name -eq ".NETCoreApp,Version=v5.0"){
				$propInnerName = $prop.Name
				$objInnerProperties = $prop.Value.PSObject.Properties
				
				foreach ($propInner in $objInnerProperties){
					$propInnerName = $propInner.Name
					#echo "PROP INNER NAME = $($propInnerName)"
					
					if($propInner.Name -Match "ONS.MPV.Controller"){
						$dependencies = $propInner.Value.PSObject.Properties
						
						foreach($dep in $dependencies){
							#echo "DEP = $($dep.Name)"
							if ($dep.Name -eq "dependencies") {								
								$objDepList = $dep.Value.PSObject.Properties
								foreach	($depProp in $objDepList){
									if ($depProp.Name -Match $nameOperator) {
										$depPropName = $depProp.Name
										$objDepList.Remove($depPropName)
										Write-Host "Removed object $($dep.Name) -- $depPropName"
									}
								}
							}
						}
					}
					
					if ($propInner.Name -Match $nameOperator) {
						$propLowName = $propInner.Name
						$objInnerProperties.Remove($propInnerName)
						Write-Host "Removed object $propInnerName -- $propLowName"
					}
				}
			}
			if ($prop.Name -Match $nameOperator) {
				$propName = $prop.Name
				$objProperties.Remove($propName)
				Write-Host "Removed object $objName -- $propName"
			}
		}
	}

	$apijson | ConvertTo-Json -Depth 100 | Set-Content -Path $inputFile -Force
		
	echo "FIM ALTERACAO JSON"
}

$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition
echo "Total de argumentos: $($args.count)"

if ($args.count -lt 1){
	echo "Precisa de ao menos um argumento."
	Break
}

for ( $i = 0; $i -lt $args.count; $i++ ) {
    echo "Argumento = $($args[$i])"
	
	#Remover DLL
	if ($args[$i] -eq 1){
		echo "INICIO DELETE DLL - SimplesDefasagem"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.SimplesDefasagem.Impl.dll"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.SimplesDefasagem.Impl.pdb"
		echo "FIM DELETE DLL - SimplesDefasagem"
		
		$value = "SimplesDefasagem"
		Get-AlterarJSON $value $scriptPath
	}
	
	if ($args[$i] -eq 2){
		echo "INICIO DELETE DLL - Muskingum"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.Muskingum.Impl.dll"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.Muskingum.Impl.pdb"
		echo "FIM DELETE DLL - Muskingum"
		
		$value = "Muskingum"
		Get-AlterarJSON $value $scriptPath
	}
	
	if ($args[$i] -eq 3){
		echo "INICIO DELETE DLL - SSARR"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.SSARR.Impl.dll"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.SSARR.Impl.pdb"
		echo "FIM DELETE DLL - SSARR"
		
		$value = "SSARR"
		Get-AlterarJSON $value $scriptPath
	}
	
	if ($args[$i] -eq 4){
		echo "INICIO DELETE DLL - Todini"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.Todini.Impl.dll"
		Remove-Item "$($scriptPath)\ONS.MPV.Business.Todini.Impl.pdb"
		echo "FIM DELETE DLL - Todini"
		
		$value = "Todini"
		Get-AlterarJSON $value $scriptPath
	}
} 

echo "Fim da execucao."