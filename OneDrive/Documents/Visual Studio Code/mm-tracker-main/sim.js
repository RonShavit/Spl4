function onFileLoad(elementId, event) {
	//SpoilerJSON = JSON.parse(event.target.result);
	SpoilerLines = event.target.result.split('\n');
	readLog();
}
function readLog() {
	simActive = true;
	document.getElementById("simLog").style.display = "inline-block";
	document.getElementById("gossips_option").value = "ON";
	
	if(document.getElementById("settings_option").value == "BLITZ" || document.getElementById("settings_option").value == "S3" || document.getElementById("settings_option").value == "S4")
		document.getElementById("Starting Song").value = "epo";
	if(document.getElementById("settings_option").value == "BLITZ")
		document.getElementById("Boss Blue Warp").value = "oaT";
	
	for(i = 0; i < SpoilerLines.length; i++) {
		if(SpoilerLines[i].indexOf("->") >= 0) {
			loc = SpoilerLines[i].split("->")[0].trim();
			item = SpoilerLines[i].split("->")[1].trim().replaceAll("*", "").replaceAll("^", "");
			if(Locations.indexOf(loc) != -1) {
				SpoilerLocToItem[loc] = item;
			}
			else if(Locations.indexOf("h_"+loc) != -1) {
				SpoilerHintDict["h_"+loc] = item;
			}
		}
	}
	if(document.getElementById("settings_option").value == "BLITZ" || document.getElementById("settings_option").value == "S3" || document.getElementById("settings_option").value == "S4")
		SpoilerLocToItem["Starting Song"] = "Epona's Song";
	if(document.getElementById("settings_option").value == "BLITZ")
		SpoilerLocToItem["Boss Blue Warp"] = "Oath to Order";
}
function onChooseFile(event, onLoadFileHandler) {
	if (typeof window.FileReader !== 'function')
		throw ("The file API isn't supported on this browser.");
	let input = event.target;
	if (!input)
		throw ("The browser does not properly implement the event object");
	if (!input.files)
		throw ("This browser does not support the `files` property of the file input.");
	if (!input.files[0])
		return undefined;
	let file = input.files[0];
	let fr = new FileReader();
	fr.onload = onLoadFileHandler;
	fr.readAsText(file);
}

function simProcessHint(gossip) {
	hint = SpoilerHintDict[gossip];
	for(var i = 0; i < hintStrings1.length; i++)
		hint = hint.replace(hintStrings1[i], "");
	
	if(hint.includes("Way of the Hero")) {
		hint = hint.replace(" is on the Way of the Hero...", "");
		
		if(simWothCounter <= 5) {
			
			if(simWothsEntered[hint] == undefined)
				simWothsEntered[hint] = 1;
			else
				simWothsEntered[hint] += 1;
			
			if(simWothsEntered[hint] == 1) {
				document.getElementById("woth_input" + simWothCounter).value = SpoilerAreaToInput[hint];
				simWothCounter += 1;
			}
		}
	}
	else if(hint.includes("foolish choice")) {
		hint = hint.replace(" is a foolish choice...", "");
		
		if(simBarrenCounter <= 5) {
			
			if(simBarrensEntered[hint] == undefined)
				simBarrensEntered[hint] = 1;
			else
				simBarrensEntered[hint] += 1;
			
			if(simBarrensEntered[hint] == 1) {
				document.getElementById("barren_input" + simBarrenCounter).value = SpoilerAreaToInput[hint];
				simBarrenCounter += 1;
			}
		}
	}
	else if(hint.includes("foolish except for its song")) {
		hint = hint.replace(" is foolish except for its song...", "");
		
		if(simBarrenCounter <= 5) {
			
			if(simBarrensEntered[hint] == undefined)
				simBarrensEntered[hint] = 1;
			else
				simBarrensEntered[hint] += 1;
			
			if(simBarrensEntered[hint] == 1) {
				document.getElementById("barren_input" + simBarrenCounter).value = "*"+SpoilerAreaToInput[hint];
				simBarrenCounter += 1;
			}
		}
	}
	else { // always or sometimes hint
		
		hint = hint.replaceAll(" not required", "");
		hint = hint.replaceAll(" required", "");
	
		if(hint.includes("Letter to Kafei Delivery")) {
			for(var i = 0; i < hintStrings2.length; i++)
				hint = hint.replace(hintStrings2[i], ":");
			hint = hint.replace("...", "");
			hint = hint.replaceAll(" then ", ":");
			hint = hint.replaceAll(" then the ", ":");
			
			let loc = hint.split(":")[0];
			
			if(Locations.indexOf("Kafei") < 0) {
				console.log("Could not fill in the hint. Kafei is not a known location.");
				return;
			}
			if(hintIndexes.indexOf("Kafei") == -1) {
				console.log("Could not fill in the hint. Kafei is not in the list of hinted locations.");
				return;
			}
			
			for(var j = 1; j <=3; j++) {
				let item = hint.split(":")[j];
				if(item.startsWith("a "))
					item = item.replace("a ", "");
				if(item.startsWith("an "))
					item = item.replace("an ", "");
				if(item.startsWith("the "))
					item = item.replace("the ", "");
				
				let loc_input = "ka"+j;
				let item_input = "x";
				if(SpoilerItemToInput[item] != undefined)
					item_input = SpoilerItemToInput[item];
				
				if(document.getElementById("hintInput").value.includes(loc_input + " \n"))
					document.getElementById("hintInput").value = document.getElementById("hintInput").value.replace(loc_input + " \n", loc_input + " " + item_input + "\n");
				else if(!document.getElementById("hintInput").value.includes(loc_input + " " + item_input + "\n"))
					document.getElementById("hintInput").value += loc_input + " " + item_input + "\n";
			}
		}
		else if(hint.includes("Ranch Sisters Defense")) {
			for(var i = 0; i < hintStrings2.length; i++)
				hint = hint.replace(hintStrings2[i], ":");
			hint = hint.replace("...", "");
			hint = hint.replaceAll(" then ", ":");
			hint = hint.replaceAll(" then the ", ":");
			
			let loc = hint.split(":")[0];
			
			if(Locations.indexOf("Aliens Defense") < 0) {
				console.log("Could not fill in the hint. Aliens Defense is not a known location.");
				return;
			}
			if(hintIndexes.indexOf("Aliens Defense") == -1) {
				console.log("Could not fill in the hint. Aliens Defense is not in the list of hinted locations.");
				return;
			}
			
			let loc_inputs = ["", "ali", "cre"]
			
			for(var j = 1; j <=2; j++) {
				let item = hint.split(":")[j];
				if(item.startsWith("a "))
					item = item.replace("a ", "");
				if(item.startsWith("an "))
					item = item.replace("an ", "");
				if(item.startsWith("the "))
					item = item.replace("the ", "");
				
				let loc_input = loc_inputs[j];
				let item_input = "x";
				if(SpoilerItemToInput[item] != undefined)
					item_input = SpoilerItemToInput[item];
				
				if(document.getElementById("hintInput").value.includes(loc_input + " \n"))
					document.getElementById("hintInput").value = document.getElementById("hintInput").value.replace(loc_input + " \n", loc_input + " " + item_input + "\n");
				else if(!document.getElementById("hintInput").value.includes(loc_input + " " + item_input + "\n"))
					document.getElementById("hintInput").value += loc_input + " " + item_input + "\n";
			}
		}
		else {
			for(var i = 0; i < hintStrings2.length; i++)
				hint = hint.replace(hintStrings2[i], ":");
			hint = hint.replace("...", "");
			hint = hint.replace("Town Archery 1", "Town Archery #1");
			hint = hint.replace("Swamp Archery 1", "Swamp Archery #1");
			hint = hint.replace("Swamp Archery 2", "Swamp Archery #2");
			hint = hint.replace("Bank Reward 3", "Bank Reward #3");
			hint = hint.replace("Beaver Race 1", "Beaver Race #1");
			
			let loc = hint.split(":")[0];
			let item = hint.split(":")[1];
			if(item.startsWith("a "))
				item = item.replace("a ", "");
			if(item.startsWith("an "))
				item = item.replace("an ", "");
			if(item.startsWith("the "))
				item = item.replace("the ", "");
			
			if(Locations.indexOf(loc) < 0) {
				console.log("Could not fill in the hint. " + loc + " is not a known location.");
				return;
			}
			if(hintIndexes.indexOf(loc) == -1) {
				console.log("Could not fill in the hint. " + loc + " is not in the list of hinted locations.");
				return;
			}
			
			let loc_input = hintInputs[hintIndexes.indexOf(loc)];
			
			let item_input = "x";
			if(SpoilerItemToInput[item] != undefined)
				item_input = SpoilerItemToInput[item];
			
			if(document.getElementById("hintInput").value.includes(loc_input + " \n"))
				document.getElementById("hintInput").value = document.getElementById("hintInput").value.replace(loc_input + " \n", loc_input + " " + item_input + "\n");
			else if(!document.getElementById("hintInput").value.includes(loc_input + " " + item_input + "\n"))
				document.getElementById("hintInput").value += loc_input + " " + item_input + "\n";
		}
	}
}

var SpoilerItemToInput = {
	"Bow Upgrade" : "bow",
	"Fire Arrow" : "fir",
	"Ice Arrow" : "ice",
	"Light Arrow" : "lig",
	"Moon's Tear" : "moo",
	"Land Title Deed" : "lan",
	"Swamp Title Deed" : "swa",
	"Mountain Title Deed" : "mou",
	"Ocean Title Deed" : "oce",
	"Bomb Bag Upgrade" : "bom",
	"Magic Bean" : "bea",
	"Room Key" : "roo",
	"Letter to Mama" : "spe",
	"Powder Keg" : "pow",
	"Pictograph Box" : "pic",
	"Lens of Truth" : "len",
	"Hookshot" : "hoo",
	"Great Fairy's Sword" : "gfs",
	"Letter to Kafei" : "let",
	"Pendant of Memories" : "pen",
	"Empty Bottle" : "bot",
	"Bottle of Red Potion" : "bot",
	"Bottle of Chateau Romani" : "bot",
	"Milk Bottle" : "bot",
	"Bottle of Gold Dust" : "gol",
	"Postman's Hat" : "pos",
	"All-Night Mask" : "all",
	"Blast Mask" : "bla",
	"Stone Mask" : "sto",
	//"Great Fairy's Mask" : "gre",
	"Deku Mask" : "dek",
	"Keaton Mask" : "kea",
	"Bremen Mask" : "bre",
	"Bunny Hood" : "bun",
	//"Don Gero's Mask" : "don",
	"Mask of Scents" : "sce",
	"Goron Mask" : "gor",
	"Romani's Mask" : "rom",
	//"Circus Leader's Mask" : "cir",
	"Kafei's Mask" : "kaf",
	"Couple's Mask" : "cou",
	"Mask of Truth" : "tru",
	"Zora Mask" : "zor",
	"Kamaro's Mask" : "kam",
	"Gibdo Mask" : "gib",
	"Garo's Mask" : "gar",
	"Captain's Hat" : "cap",
	"Giant's Mask" : "gia",
	"Fierce Deity's Mask" : "fie",
	"Sword Upgrade" : "swo",
	"Mirror Shield" : "mir",
	"Magic Power Upgrade" : "mag",
	"Wallet Upgrade" : "wal",
	"Song of Healing" : "soh",
	"Epona's Song" : "epo",
	"Song of Storms" : "sos",
	"Sonata of Awakening" : "son",
	"Goron Lullaby" : "lul",
	"New Wave Bossa Nova" : "nov",
	"Elegy of Emptiness" : "ele",
	"Oath to Order" : "oat"
}

var SpoilerAreaToInput = {
	"Clock Tower Rooftop" : "clo",
	"South Clock Town" : "sct",
	"North Clock Town" : "nct",
	"West Clock Town" : "wct",
	"Laundry Pool" : "lau",
	"East Clock Town" : "ect",
	"Stock Pot Inn" : "inn",
	"Termina Field" : "fie",
	"Road to Southern Swamp" : "rsw",
	"Southern Swamp" : "swa",
	"Deku Palace" : "pal",
	"Woodfall" : "woo",
	"Milk Road" : "mil",
	"Romani Ranch" : "ran",
	"Mountain Village" : "mou",
	"Twin Islands" : "twi",
	"Goron Village" : "gor",
	"Path to Snowhead" : "psn",
	"Great Bay Coast" : "gre",
	"Zora Cape" : "cap",
	"Zora Hall" : "hal",
	"Pirates' Fortress Exterior" : "ext",
	"Pirates' Fortress Sewer" : "sew",
	"Pirates' Fortress Interior" : "int",
	"Pinnacle Rock" : "pin",
	"Road to Ikana" : "rik",
	"Ikana Graveyard" : "gra",
	"Ikana Canyon" : "can",
	"Secret Shrine" : "shr",
	"Beneath the Well" : "wel",
	"Ikana Castle" : "cas",
	"Stone Tower" : "sto",
	"Woodfall Temple" : "wft",
	"Snowhead Temple" : "sht",
	"Great Bay Temple" : "gbt",
	"Stone Tower Temple" : "stt"
}
