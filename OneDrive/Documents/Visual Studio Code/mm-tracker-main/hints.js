function handleAlternateHintInput() {
	var lines = document.getElementById("hintInput").value.split('\n');
	var str = "";
	for (var i= 0; i < hintInputs.length; i++) {
		for (var j=0; j < lines.length; j++) {
			if (lines[j].startsWith(hintInputs[i]+" ")) {
				for (var k = 0; k < inputs.length; k++) {
					str = inputs[k];
					str = capitalizeFirstLetter(str);
					
					if (k == 0) {
						if (lines[j].endsWith(" " + inputs[k]) && Check[hintIndexes[i]] == "unknown") {
							thisIsHinted = true; 
							document.getElementById("text_" + hintIndexes[i]).dispatchEvent(new Event('mousedown')); 
							thisIsHinted = false; 
						}
					}
					else {
						if (lines[j].endsWith(" " + inputs[k])) {
							if (Check[hintIndexes[i]] == "unknown") {
								hintedInput = str.toLowerCase();
								document.getElementById(hintIndexes[i]).value = str;
							}
							else
								Hinted[hintIndexes[i]] = true;
						}
					}
				}
			}
		}
	}
}

function woth_and_barren_processing() {
	for(var i = 1; i <= AreaNames.length-1; i++) {
		Area[i] = "normal";
	}
	
	wothAreas = ["", "", "", "", "", ""];
	
	// loops the 5 woth entries
	for (var i = 1; i <= 5; i++) {
		var str = document.getElementById("woth_input" + i).value;
		str = str.replace("2",""); // if there's a 2, ignore it
		
		for(var j = 1; j < AreaNames.length; j++) {
			if (str == areaInputs[j]) {
				if (Area[j] == "4woth") {Area[j] = "5woth";} 
				else if (Area[j] == "3woth") {Area[j] = "4woth";} 
				else if (Area[j] == "2woth") {Area[j] = "3woth";} 
				else if (Area[j] == "woth") {Area[j] = "2woth";} 
				else {Area[j] = "woth";}
				wothAreas[i] = AreaNames[j];
			}
		}
	}
	for (var i = 1; i <= 4; i++) {
		var str2 = document.getElementById("barren_input" + i).value;
		str2 = str2.replace("2",""); // if there's a 2, ignore it
		
		var foolish_except_song = false;
		if(str2.indexOf("*") != -1) {
			str2 = str2.replace("*",""); // if there's a *, set the foolish except song flag
			foolish_except_song = true;
		}
		
		for(var j = 1; j < AreaNames.length; j++) {
			if(str2 == areaInputs[j] && !foolish_except_song) {
				Area[j] = "barren";
			}
			else if (str2 == areaInputs[j] && foolish_except_song) {
				Area[j] = "barren_except_song";
			}
		}
	}
	
	// add the area woth title or change it
	for (var i = 1; i <= AreaNames.length; i++) {
		
		var counterCopyList = []
		if(woth1 == AreaNames[i]) {
			counterCopyList.push(1);
		}
		if(woth2 == AreaNames[i]) {
			counterCopyList.push(2);
		}
		if(woth3 == AreaNames[i]) {
			counterCopyList.push(3);
		}
		if(woth4 == AreaNames[i]) {
			counterCopyList.push(4);
		}
		if(woth5 == AreaNames[i]) {
			counterCopyList.push(5);
		}
		
		if (Area[i] == "woth" && AreaWotHAge[i] != 1) {
			if (AreaWotHAge[i] < 1) {
				if (woth1 == "unknown" || !Area[AreaNames.indexOf(woth1)].includes("woth")) {document.getElementById("woth1_title").innerHTML = AreaNames[i]; document.getElementById("woth1_title").style.color= WotHColors[1]; woth1 = AreaNames[i];}
				else if (woth2 == "unknown" || !Area[AreaNames.indexOf(woth2)].includes("woth")) {document.getElementById("woth2_title").innerHTML = AreaNames[i]; document.getElementById("woth2_title").style.color= WotHColors[2]; woth2 = AreaNames[i];}
				else if (woth3 == "unknown" || !Area[AreaNames.indexOf(woth3)].includes("woth")) {document.getElementById("woth3_title").innerHTML = AreaNames[i]; document.getElementById("woth3_title").style.color= WotHColors[3]; woth3 = AreaNames[i];}
				else if (woth4 == "unknown" || !Area[AreaNames.indexOf(woth4)].includes("woth")) {document.getElementById("woth4_title").innerHTML = AreaNames[i]; document.getElementById("woth4_title").style.color= WotHColors[4]; woth4 = AreaNames[i];}
				else if (woth5 == "unknown" || !Area[AreaNames.indexOf(woth5)].includes("woth")) {document.getElementById("woth5_title").innerHTML = AreaNames[i]; document.getElementById("woth5_title").style.color= WotHColors[5]; woth5 = AreaNames[i];}
			}
			
			for(var j = 1; j < counterCopyList.length; j++) {
				if(counterCopyList[j] == 1) {
					woth1 = "unknown";
					document.getElementById("woth1_title").innerHTML = "";
				}
				else if(counterCopyList[j] == 2) {
					woth2 = "unknown";
					document.getElementById("woth2_title").innerHTML = "";
				}
				else if(counterCopyList[j] == 3) {
					woth3 = "unknown";
					document.getElementById("woth3_title").innerHTML = "";
				}
				else if(counterCopyList[j] == 4) {
					woth4 = "unknown";
					document.getElementById("woth4_title").innerHTML = "";
				}
				else if(counterCopyList[j] == 5) {
					woth5 = "unknown";
					document.getElementById("woth5_title").innerHTML = "";
				}
			}
			
			AreaWotHAge[i] = 1;
		}
		else if (Area[i] == "2woth" && AreaWotHAge[i] != 2) {
			if (AreaWotHAge[i] < 2)	{			
				if (woth1 == "unknown" || !Area[AreaNames.indexOf(woth1)].includes("woth")) {woth1 = AreaNames[i];}
				else if (woth2 == "unknown" || !Area[AreaNames.indexOf(woth2)].includes("woth")) {woth2 = AreaNames[i];}
				else if (woth3 == "unknown" || !Area[AreaNames.indexOf(woth3)].includes("woth")) {woth3 = AreaNames[i];}
				else if (woth4 == "unknown" || !Area[AreaNames.indexOf(woth4)].includes("woth")) {woth4 = AreaNames[i];}
				else if (woth5 == "unknown" || !Area[AreaNames.indexOf(woth5)].includes("woth")) {woth5 = AreaNames[i];}
			}
			
			for(var j = 2; j < counterCopyList.length; j++) {
				if(counterCopyList[j] == 1) {
					woth1 = "unknown";
				}
				else if(counterCopyList[j] == 2) {
					woth2 = "unknown";
				}
				else if(counterCopyList[j] == 3) {
					woth3 = "unknown";
				}
				else if(counterCopyList[j] == 4) {
					woth4 = "unknown";
				}
				else if(counterCopyList[j] == 5) {
					woth5 = "unknown";
				}
			}
			
			AreaWotHAge[i] = 2;
		}
		else if (Area[i] == "3woth" && AreaWotHAge[i] != 3) {		
			if (AreaWotHAge[i] < 3)	{
				if (woth1 == "unknown" || !Area[AreaNames.indexOf(woth1)].includes("woth")) {woth1 = AreaNames[i];}
				else if (woth2 == "unknown" || !Area[AreaNames.indexOf(woth2)].includes("woth")) {woth2 = AreaNames[i];}
				else if (woth3 == "unknown" || !Area[AreaNames.indexOf(woth3)].includes("woth")) {woth3 = AreaNames[i];}
				else if (woth4 == "unknown" || !Area[AreaNames.indexOf(woth4)].includes("woth")) {woth4 = AreaNames[i];}
				else if (woth5 == "unknown" || !Area[AreaNames.indexOf(woth5)].includes("woth")) {woth5 = AreaNames[i];}
			}
			
			for(var j = 3; j < counterCopyList.length; j++) {
				if(counterCopyList[j] == 1) {
					woth1 = "unknown";
				}
				else if(counterCopyList[j] == 2) {
					woth2 = "unknown";
				}
				else if(counterCopyList[j] == 3) {
					woth3 = "unknown";
				}
				else if(counterCopyList[j] == 4) {
					woth4 = "unknown";
				}
				else if(counterCopyList[j] == 5) {
					woth5 = "unknown";
				}
			}
			AreaWotHAge[i] = 3;
		}
		else if (Area[i] == "4woth" && AreaWotHAge[i] != 4) {
			if (AreaWotHAge[i] < 4)	{			
				if (woth1 == "unknown" || !Area[AreaNames.indexOf(woth1)].includes("woth")) {woth1 = AreaNames[i];}
				else if (woth2 == "unknown" || !Area[AreaNames.indexOf(woth2)].includes("woth")) {woth2 = AreaNames[i];}
				else if (woth3 == "unknown" || !Area[AreaNames.indexOf(woth3)].includes("woth")) {woth3 = AreaNames[i];}
				else if (woth4 == "unknown" || !Area[AreaNames.indexOf(woth4)].includes("woth")) {woth4 = AreaNames[i];}
				else if (woth5 == "unknown" || !Area[AreaNames.indexOf(woth5)].includes("woth")) {woth5 = AreaNames[i];}
			}
			
			for(var j = 4; j < counterCopyList.length; j++) {
				if(counterCopyList[j] == 1) {
					woth1 = "unknown";
				}
				else if(counterCopyList[j] == 2) {
					woth2 = "unknown";
				}
				else if(counterCopyList[j] == 3) {
					woth3 = "unknown";
				}
				else if(counterCopyList[j] == 4) {
					woth4 = "unknown";
				}
				else if(counterCopyList[j] == 5) {
					woth5 = "unknown";
				}
			}
			AreaWotHAge[i] = 4;
		}
		else if (Area[i] == "5woth" && AreaWotHAge[i] != 5) {		
			if (AreaWotHAge[i] < 5) {
				if (woth1 == "unknown" || !Area[AreaNames.indexOf(woth1)].includes("woth")) {woth1 = AreaNames[i];}
				else if (woth2 == "unknown" || !Area[AreaNames.indexOf(woth2)].includes("woth")) {woth2 = AreaNames[i];}
				else if (woth3 == "unknown" || !Area[AreaNames.indexOf(woth3)].includes("woth")) {woth3 = AreaNames[i];}
				else if (woth4 == "unknown" || !Area[AreaNames.indexOf(woth4)].includes("woth")) {woth4 = AreaNames[i];}
				else if (woth5 == "unknown" || !Area[AreaNames.indexOf(woth5)].includes("woth")) {woth5 = AreaNames[i];}
			}
			
			for(var j = 5; j < counterCopyList.length; j++) {
				if(counterCopyList[j] == 1) {
					woth1 = "unknown";
				}
				else if(counterCopyList[j] == 2) {
					woth2 = "unknown";
				}
				else if(counterCopyList[j] == 3) {
					woth3 = "unknown";
				}
				else if(counterCopyList[j] == 4) {
					woth4 = "unknown";
				}
				else if(counterCopyList[j] == 5) {
					woth5 = "unknown";
				}
			}
			AreaWotHAge[i] = 5;
		}
		else if (Area[i] == "normal" || Area[i] == "barren" || Area[i] == "barren_except_song") {
			if (woth1 == AreaNames[i]) {woth1 = "unknown";document.getElementById("woth1_title").innerHTML = "";for (var k = 1; k < 7; k++){document.getElementById("woth1_text"+k).innerHTML = "";}}
			if (woth2 == AreaNames[i]) {woth2 = "unknown";document.getElementById("woth2_title").innerHTML = "";for (var k = 1; k < 7; k++){document.getElementById("woth2_text"+k).innerHTML = "";}}
			if (woth3 == AreaNames[i]) {woth3 = "unknown";document.getElementById("woth3_title").innerHTML = "";for (var k = 1; k < 7; k++){document.getElementById("woth3_text"+k).innerHTML = "";}}
			if (woth4 == AreaNames[i]) {woth4 = "unknown";document.getElementById("woth4_title").innerHTML = "";for (var k = 1; k < 7; k++){document.getElementById("woth4_text"+k).innerHTML = "";}}
			if (woth5 == AreaNames[i]) {woth5 = "unknown";document.getElementById("woth5_title").innerHTML = "";for (var k = 1; k < 7; k++){document.getElementById("woth5_text"+k).innerHTML = "";}}
			AreaWotHAge[i] = 0;	
		}
	}
	
	// process woth hints
	for (var i = 1; i <= AreaNames.length; i++) {
		var wothRowNumber = 1;
		var wothNumber = 0;
		var numAreaWotHItemsFound = 1;
		var tempbool = false;
	
		// if it's a woth area
		if (woth1 == AreaNames[i] || woth2 == AreaNames[i] || woth3 == AreaNames[i] || woth4 == AreaNames[i] || woth5 == AreaNames[i]) {
			if (woth1 == AreaNames[i]) {wothNumber = 1;}
			else if (woth2 == AreaNames[i]) {wothNumber = 2;}
			else if (woth3 == AreaNames[i]) {wothNumber = 3;}
			else if (woth4 == AreaNames[i]) {wothNumber = 4;}
			else if (woth5 == AreaNames[i]) {wothNumber = 5;}
			var tempArray = [];
			document.getElementById("woth" + wothNumber + "_text" + 1).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 2).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 3).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 4).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 5).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 6).innerHTML="";
			document.getElementById("woth" + wothNumber + "_text" + 7).innerHTML="";
			
			for (var k = 1; k < Items.length; k++) {
				
				// Cannot be hinted WotH
				if (Items[k] == "giants_mask") {continue;}
				if (Items[k] == "fiercedeity_mask") {continue;}
				if (Items[k] == Check["Starting Song"]) {continue;} // it doesn't have an area
				if (Items[k] == Check["Keaton Quiz"]) {continue;} // multi-region check. none of them can be woth
				if (Items[k] == Check["Postbox"]) {continue;} // multi-region check. none of them can be woth
				if (Items[k] == Check["Woodfall Map Purchase"]) {continue;} // multi-region check. none of them can be woth
				if (Items[k] == "oath") {continue;} // it can't lock an item, so cannot be woth
				
				// Logically useless
				if (Items[k] == "sword1") {continue;}
				if (Items[k] == "sword2") {continue;}
				if (Items[k] == "great_fairy_sword") {continue;}
				if (Items[k] == "circusleaders_mask") {continue;}
				if (Items[k] == "stone_mask") {continue;}
				if (Items[k] == "greatfairy_mask") {continue;}
				if (Items[k] == "pendant_of_memories") {continue;}
				if (Items[k] == "dongero_mask") {continue;}

				if (alwaysHints.indexOf(ItemLocation[Items[k]]) == -1 && ItemLocation[Items[k]] != null && (Hinted[ItemLocation[Items[k]]] == false || typeof Hinted[ItemLocation[Items[k]]] == "undefined")) 
				{
					var blue_warp_song = false;
					if(ItemLocation[Items[k]] == "Boss Blue Warp" && ((AreaNames[i] == "WFT" && Items[k] != "sonata") || (AreaNames[i] == "SHT" && Items[k] != "lullaby") || (AreaNames[i] == "GBT" && Items[k] != "nwbn") || (AreaNames[i] == "STT" && Items[k] != "elegy")))
						blue_warp_song = true;
					
					if(ItemLocation[Items[k]] == "Biggest Bomb Bag Purchase" && ((AreaNames[i] == "Swamp" && ((Items[k] == "deku_mask") || (Items[k] == "moons_tear") || (Items[k] == "land_title_deed") || (Items[k] == "swamp_title_deed"))) || (AreaNames[i] == "Gor Village" && Items[k] == "goron_mask")))
						continue;
					
					if ((Locations.indexOf(ItemLocation[Items[k]]) >= AreaIndexes[i-1] && Locations.indexOf(ItemLocation[Items[k]]) < AreaIndexes[i]) 
					|| Locations.indexOf(ItemLocation[Items[k]]) == SongIndexes[i-1] || blue_warp_song) 
					{
						document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).innerHTML = "<img id = 'wothMajor" + wothNumber + "_" + k + "'" + " class = 'wothMajorImages' src=" + ItemImages[k] + " data-item = '" + Items[k] + "' onmousedown = 'markWothItem(this)'>";
						
						if(ManualWotHItems[Items[k]]) {
							document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).style.display = "block";
							document.getElementById("wothMajor" + wothNumber + "_" + k).className = "manualWothImages";
						}
						else if(ManualNotWotHItems[Items[k]]) {
							document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).style.display = "none";
							document.getElementById("wothMajor" + wothNumber + "_" + k).className = "notWothItemImages";
						}
						else {
							document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).style.display = "block";
							document.getElementById("wothMajor" + wothNumber + "_" + k).className = "wothMajorImages";
						}
						
						document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).innerHTML += "<span onmousedown = 'markWothItemArrow(this)' id = 'wothItemArrow" + wothNumber + "_" + k + "' data-item = '" + Items[k] + "' class = 'woth_item_arrow'> &#8594; </span>"; 
							
						if(MarkedWotHItemArrow == "wothItemArrow" + wothNumber + "_" + k)
							document.getElementById("wothItemArrow" + wothNumber + "_" + k).style.color = "chartreuse";
						
						if(ManualWotHItemLocked[Items[k]] != undefined) {
							for(var q = 0; q < ManualWotHItemLocked[Items[k]].length; q++) {
								for (var j = 0; j < Items.length; j++) {
									if(Items[j] == ManualWotHItemLocked[Items[k]][q]) {
										document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).innerHTML += "<img id = 'wothMinor" + wothNumber + "_" + wothRowNumber + "_" + j + "'" + "class = 'wothImages' data-item = '" + Items[j] + "' data-parent = '" + Items[k] + "'onmousedown = 'markMinorWothItem(this)' src=" + ItemImages[j] + ">";
										
										if (ManualWotHMinorItems[Items[j]]) {
											document.getElementById("wothMinor" + wothNumber + "_" + wothRowNumber + "_" + j).style.setProperty("-webkit-filter", "drop-shadow(0px 0px 5px yellow)"); 
										} 
									}
								}
							}
						}
						if(ManualWotHItemPutInLogic[Items[k]] != undefined) {
							for(var q = 0; q < ManualWotHItemPutInLogic[Items[k]].length; q++) {
								for (var j = 0; j < Items.length; j++) {
									if(Items[j] == ManualWotHItemPutInLogic[Items[k]][q]) {
										document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).innerHTML += "<img id = 'wothMinor" + wothNumber + "_" + wothRowNumber + "_" + j + "'" + "class = 'putInLogicByWothItemImages' data-item = '" + Items[j] + "' data-parent = '" + Items[k] + "'onmousedown = 'markMinorWothItem(this)' src=" + ItemImages[j] + ">";
										
										if (ManualWotHMinorItems[Items[j]]) {
										document.getElementById("wothMinor" + wothNumber + "_" + wothRowNumber + "_" + j).style.setProperty("-webkit-filter", "drop-shadow(0px 0px 5px yellow)"); 
										} 
									}
								}
							}
						}
						
						document.getElementById("woth" + wothNumber + "_text" + wothRowNumber).innerHTML += " <br>"; 
						wothRowNumber += 1; 
						tempArray.push(Items[k]); 
					}
				} 	
			}
		}	
	}
	
	// process barren hints
	for (var i = 1; i <= AreaNames.length; i++) {
		
		if (Area[i] == "barren" || Area[i] == "barren_except_song") {
			
			for (var j = AreaIndexes[i-1]; j < AreaIndexes[i]; j++) {
				if(Locations[j].includes("h_"))
					continue;
				if((AreaNames[i] == "WFT" || AreaNames[i] == "SHT" || AreaNames[i] == "GBT" || AreaNames[i] == "STT") && j == AreaIndexes[i]-1)
					continue;
				
				if(Check[Locations[j]] == "unknown") {
					if(AreaNames[i] == "Gor Village" && Locations[j] == "Biggest Bomb Bag Purchase") {
						continue;
					}
					
					//document.getElementById("text_" + Locations[j]).style.border = "solid 1px red";
					//document.getElementById("text_" + Locations[j]).dispatchEvent(new Event('mousedown'));
					document.getElementById(Locations[j]).value = "x";
				}
			}
			
			if(Area[i] == "barren") {
				if(SongIndexes[i-1] != 1000) {
					if(Check[Locations[SongIndexes[i-1]]] == "unknown") {
						document.getElementById("text_" + Locations[SongIndexes[i-1]]).style.border = "solid 1px red";
					}
				}
			}
			
			if(AreaNames[i] == "Milk Road" && Check["Keaton Quiz"] == "unknown") {
				//document.getElementById("text_" + "Keaton Quiz").style.border = "solid 1px red";
				//document.getElementById("text_" + "Keaton Quiz").dispatchEvent(new Event('mousedown'));
				document.getElementById("Keaton Quiz").value = "x";
			}
			if(AreaNames[i] == "NCT" && Check["Woodfall Map Purchase"] == "unknown") {
				//document.getElementById("text_" + "Woodfall Map Purchase").style.border = "solid 1px red";
				//document.getElementById("text_" + "Woodfall Map Purchase").dispatchEvent(new Event('mousedown'));
				document.getElementById("Woodfall Map Purchase").value = "x";
			}
			if((AreaNames[i] == "NCT" || AreaNames[i] == "ECT") && Check["Postbox"] == "unknown") {
				//document.getElementById("text_" + "Postbox").style.border = "solid 1px red";
				//document.getElementById("text_" + "Postbox").dispatchEvent(new Event('mousedown'));
				document.getElementById("Postbox").value = "x";
			}
		}
	}
}

function markWothItem(x) {
	if(event.which == 1 && ManualWotHItems[x.getAttribute("data-item")] != true) {
		ManualWotHItems[x.getAttribute("data-item")] = true;
		ManualNotWotHItems[x.getAttribute("data-item")] = false;
	}
	else if(event.which == 3 && ManualNotWotHItems[x.getAttribute("data-item")] != true) {
		ManualNotWotHItems[x.getAttribute("data-item")] = true;
		ManualWotHItems[x.getAttribute("data-item")] = false;
	}
	else {
		ManualWotHItems[x.getAttribute("data-item")] = false;
		ManualNotWotHItems[x.getAttribute("data-item")] = false;
	}
	Update();
}

function markWothItemArrow(x) {
	if(MarkedWotHItemArrow == x.id)
		MarkedWotHItemArrow = null;
	else
		MarkedWotHItemArrow = x.id;
	Update();
}

function markMinorWothItem(x) {
	var theItem = x.getAttribute("data-item");
	var theParent = x.getAttribute("data-parent");
	
	if(event.which == 1) {
		if(ManualWotHMinorItems[theItem] == undefined || ManualWotHMinorItems[theItem] == false)
			ManualWotHMinorItems[theItem] = true;
		else
			ManualWotHMinorItems[theItem] = false;
	}
	else if(event.which == 2) {
		if(ManualWotHItemLocked[theParent].includes(theItem)) {
			ManualWotHItemLocked[theParent].splice(ManualWotHItemLocked[theParent].indexOf(theItem), 1);
			ManualWotHItemPutInLogic[theParent].push(theItem);
		}
		else if(ManualWotHItemPutInLogic[theParent].includes(theItem)) {
			ManualWotHItemPutInLogic[theParent].splice(ManualWotHItemPutInLogic[theParent].indexOf(theItem), 1);
			ManualWotHItemLocked[theParent].push(theItem);
		}
	}
	else if(event.which == 3 && ManualNotWotHItems[theItem] != true) {
		if(ManualWotHItemLocked[theParent].includes(theItem))
			ManualWotHItemLocked[theParent].splice(ManualWotHItemLocked[theParent].indexOf(theItem), 1);
		if(ManualWotHItemPutInLogic[theParent].includes(theItem))
			ManualWotHItemPutInLogic[theParent].splice(ManualWotHItemPutInLogic[theParent].indexOf(theItem), 1);
		
		ManualWotHMinorItems[theItem] = false;
	}
	Update();
}

function resetWoth(num) {
	for (var i = 1; i <= 34; i++) {
		if(AreaNames[i] == wothAreas[num]) {
			for (var k = 0; k < Items.length; k++) {
				if(ManualNotWotHItems[Items[k]] && ((Locations.indexOf(ItemLocation[Items[k]]) >= AreaIndexes[i-1] && Locations.indexOf(ItemLocation[Items[k]]) < AreaIndexes[i]) || Locations.indexOf(ItemLocation[Items[k]]) == SongIndexes[i-1])) {
					ManualNotWotHItems[Items[k]] = false;
				}
			}
		}
	}	
}