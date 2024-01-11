var AreaIndexes = [
	0,0,5,13,22,26,38,45,67,74,85,89,92,97,107,117,122,
	132,134,144,152,159,162,166,171,174,178,183,191,196,199,200,203,
	212,227,239,264
];
var SongIndexes = [
	265/*skull kid*/,1000,1000,1000,1000,1000,1000,1000,1000,267/*swamp statue*/,266/*monkey song*/,
	1000,1000,269/*eponas*/,1000,1000,270/*goron*/,1000,271/*eggs*/,
	1000,1000,1000,1000,1000,1000,1000,272/*grave*/,1000,1000,1000,
	273/*castle*/,1000,1000,1000,1000,1000
];
var lastItem = 264;

var AreaNames = [
	"", "Clock Tower", "SCT", "NCT", "WCT", "Laundry", "ECT", "Stock Pot", "Field",
	"Road Swamp", "Swamp", "Palace", "Woodfall",
	"Milk Road", "Ranch", 
	"Mtn Village", "Twin Islands", "Gor Village", "Path Snow",
	"Great Bay", "Zora Cape", "Zora Hall", "PF Exterior", "PF Sewers", "PF Interior", "Pinnacle",
	"Road Ikana", "Graveyard", "Canyon", "Shrine", "Well", "Castle", "Stone Tower", 
	"WFT", "SHT", "GBT", "STT",
	"Songs"
];
var AreaNamesShort = [
	"", "Clock", "SCT", "NCT", "WCT", "Lau", "ECT", "StoPot", "Field",
	"RSwamp", "Swamp", "Palace", "Woodfall",
	"Milk Road", "Ranch", 
	"MtnVil", "TwinIsle", "GorVil", "PSnow",
	"GreBay", "ZorCape", "ZorHall", "PirExt", "PirSew", "PirInt", "Pin",
	"RIkana", "Grave", "Canyon", "Shrine", "Well", "Castle", "StoTower", 
	"WFT", "SHT", "GBT", "STT",
	"Songs"
];

var areaInputs = [
	"", "clo", "sct", "nct", "wct", "lau", "ect", "inn", "fie", "rsw", "swa", "pal", "woo", "mil", "ran", "mou", "twi", "gor", "psn", "gre", "cap", "hal", "ext", "sew", "int", "pin", "rik", "gra", "can", "shr", "wel", "cas", "sto", "wft", "sht", "gbt", "stt"
];

var AreaImages = [
	"", "sct.png", "nct.png", "wct.png", "laun.png", "ect.png", "stockpot.png", "field3.png", 
	"roadtosouthern.png", "swamp.png", "palace.png", "woodfall.png",
	"milkroad2.png", "ranch.jpg", "mountainvillage.png", "twinislands.png", "goronvillage.png",
	"pathsnow.png", "gbc.png", "zoracape.png", "zoracape.png", "pirate2.png", "pirate2.png", "pirate2.png", "gbc.png", "ikana.png", "graveyard.jpg", "ikana.png", "shrine.png", "well.png", "castle.png", "stonetower.png"];

var Known = [];
var Check = {};
var ItemLocations = {};
var ItemLocation = {};
var Items = [
	"junk", "bow1", "bow2", "bow3", "fire_arrow", "ice_arrow", "light_arrow", "moons_tear", "land_title_deed", "swamp_title_deed", "mountain_title_deed", "ocean_title_deed", "bomb1", "bomb2", "bomb3", "magic_bean1", "magic_bean2", "room_key", "special_delivery", "powder_keg", "pictobox", "lens", "hookshot", "great_fairy_sword", "letter_to_kafei", "pendant_of_memories", "bottle1", "bottle2", "bottle3", "bottle4", "bottle5", "bottle_gold_dust", "postmans_hat", "allnight_mask", "blast_mask", "stone_mask", "greatfairy_mask", "deku_mask", "keaton_mask", "bremen_mask", "bunny_hood", "dongero_mask", "mask_of_scents", "goron_mask", "romani_mask", "circusleaders_mask", "kafei_mask", "couples_mask", "mask_of_truth", "zora_mask", "kamaro_mask", "gibdo_mask", "garo_mask", "captains_hat", "giants_mask", "fiercedeity_mask", "sword1", "sword2", "mirror_shield", "magic1", "magic2", "wallet1", "wallet2", "song_of_healing", "eponas_song", "song_of_storms", "sonata", "lullaby", "nwbn", "elegy", "oath" 
];
var ItemNames = [
	"junk", "Bow", "Bow", "Bow", "Fire Arrow", "Ice Arrow", "Light Arrow", "Moon's Tear", "Land Deed", "Swamp Deed", "Mtn Deed", "Ocean Deed", "Bomb Bag", "Bomb Bag", "Bomb Bag", "Bean", "Bean", "Room Key", "Spec Delivery", "Powder Keg", "Pictobox", "Lens", "Hookshot", "GFS", "Letter Kafei", "Pendant", "Bottle", "Bottle", "Bottle", "Bottle", "Bottle", "Bottle Gold", "Postmans", "All-Night", "Blast", "Stone", "Great Fairy", "Deku", "Keaton", "Bremen", "Bunny", "Don Gero", "Scents", "Goron", "Romani", "Circus", "Kafei", "Couples", "Truth", "Zora", "Kamaro", "Gibdo", "Garo", "Captains", "Giants", "Fierce Deity", "Sword", "Sword", "Mirror", "Magic", "Magic", "Wallet", "Wallet", "Healing", "Epona's", "Storms", "Sonata", "Lullaby", "NWBN", "Elegy", "Oath"
];
var ItemImages = [
	"", "./images/Hero's%20Bow.png", "./images/Hero's%20Bow.png", "./images/Hero's%20Bow.png", "./images/Fire%20Arrow.png", "./images/Ice%20Arrow.png", "./images/Light%20Arrow.png", "./images/Moon's%20Tear.png", "./images/Land%20Title%20Deed.png", "./images/Swamp%20Title%20Deed.png", "./images/Mountain%20Title%20Deed.png", "./images/Ocean%20Title%20Deed.png", "./images/Bomb.png", "./images/Bomb.png", "./images/Bomb.png", "./images/Magic%20Beans.png", "./images/Magic%20Beans.png", "./images/Room%20Key.png", "./images/Special%20Delivery%20to%20Mama.png", "./images/Powder%20Keg.png", "./images/Pictograph%20Box.png", "./images/Lens%20of%20Truth.png", "./images/Hookshot.png", "./images/Great%20Fairy's%20Sword.png", "./images/Letter%20to%20Kafei.png", "./images/Pendant%20of%20Memories.png", "./images/Empty%20Bottle.png", "./images/Empty%20Bottle.png", "./images/Empty%20Bottle.png", "./images/Empty%20Bottle.png", "./images/Empty%20Bottle.png", "./images/Gold%20Dust.png", "./images/Postman's%20Hat.png", "./images/All-Night%20Mask.png", "./images/Blast%20Mask.png", "./images/Stone%20Mask.png", "./images/Great%20Fairy's%20Mask.png", "./images/Deku%20Mask.png", "./images/Keaton%20Mask.png", "./images/Bremen%20Mask.png", "./images/Bunny%20Hood.png", "./images/Don%20Gero's%20Mask.png", "./images/Mask%20of%20Scents.png", "./images/Goron%20Mask.png", "./images/Romani's%20Mask.png", "./images/Circus%20Leader's%20Mask.png", "./images/Kafei's%20Mask.png", "./images/Couple's%20Mask.png", "./images/Mask%20of%20Truth.png", "./images/Zora%20Mask.png", "./images/Kamaro's%20Mask.png", "./images/Gibdo%20Mask.png", "./images/Garo's%20Mask.png", "./images/Captain's%20Hat.png", "./images/Giant's%20Mask.png", "./images/Fierce%20Deity's%20Mask.png", "./images/Razor%20Sword.png", "./images/Gilded%20Sword.png", "./images/Mirror%20Shield.png", "./images/Magic.png", "./images/Magic.png", "./images/Adult's%20Wallet.png", "./images/Giant's%20Wallet.png", "./images/songs/healing2.png", "./images/songs/eponas.png", "./images/songs/storms.png", "./images/songs/sonata.png", "./images/songs/lullaby.png", "./images/songs/nwbn.png", "./images/songs/elegy.png", "./images/songs/oath.png" 
];

var Items2 = [
	"junk", "bow", "fire_arrow", "ice_arrow", "light_arrow", "moons_tear", "land_title_deed", "swamp_title_deed", "mountain_title_deed", "ocean_title_deed", "bomb", "magic_bean", "room_key", "special_delivery", "powder_keg", "pictobox", "lens", "hookshot", "great_fairy_sword", "letter_to_kafei", "pendant_of_memories", "bottle", "bottle_gold_dust", "postmans_hat", "allnight_mask", "blast_mask", "stone_mask", "greatfairy_mask", "deku_mask", "keaton_mask", "bremen_mask", "bunny_hood", "dongero_mask", "mask_of_scents", "goron_mask", "romani_mask", "circusleaders_mask", "kafei_mask", "couples_mask", "mask_of_truth", "zora_mask", "kamaro_mask", "gibdo_mask", "garo_mask", "captains_hat", "giants_mask", "fiercedeity_mask", "sword", "mirror_shield", "magic", "wallet", "song_of_healing", "eponas_song", "song_of_storms", "sonata", "lullaby", "nwbn", "elegy", "oath"
];
var checkSummaryText = [
	"junk", "bow1", "bow2", "bow3", "fire_arrow", "ice_arrow", "light_arrow", "moons_tear", "land_title_deed", "swamp_title_deed", "mountain_title_deed", "ocean_title_deed", "bomb1", "bomb2", "bomb3", "magic_bean1", "magic_bean2", "room_key", "special_delivery", "powder_keg", "pictobox", "lens", "hookshot", "great_fairy_sword", "letter_to_kafei", "pendant_of_memories", "bottle1", "bottle2", "bottle3", "bottle4", "bottle5", "bottle_gold_dust", "postmans_hat", "allnight_mask", "blast_mask", "stone_mask", "greatfairy_mask", "deku_mask", "keaton_mask", "bremen_mask", "bunny_hood", "dongero_mask", "mask_of_scents", "goron_mask", "romani_mask", "circusleaders_mask", "kafei_mask", "couples_mask", "mask_of_truth", "zora_mask", "kamaro_mask", "gibdo_mask", "garo_mask", "captains_hat", "giants_mask", "fiercedeity_mask", "sword1", "sword2", "mirror_shield", "magic1", "magic2", "wallet1", "wallet2"
];
var SingletonItems = [
	"fire_arrow", "ice_arrow", "light_arrow", "moons_tear", "land_title_deed", "swamp_title_deed", "mountain_title_deed", "ocean_title_deed", "room_key", "special_delivery", "powder_keg", "pictobox", "lens", "hookshot", "great_fairy_sword", "mirror_shield", "letter_to_kafei", "pendant_of_memories", "bottle_gold_dust", "postmans_hat", "allnight_mask", "blast_mask", "stone_mask", "greatfairy_mask", "deku_mask", "keaton_mask", "bremen_mask", "bunny_hood", "dongero_mask", "mask_of_scents", "goron_mask", "romani_mask", "circusleaders_mask", "kafei_mask", "couples_mask", "mask_of_truth", "zora_mask", "kamaro_mask", "gibdo_mask", "garo_mask", "captains_hat", "giants_mask", "fiercedeity_mask", "song_of_healing", "eponas_song", "song_of_storms", "sonata", "lullaby", "nwbn", "elegy", "oath"
];
var SongItems = [
	"Starting Song", "Skull Kid Song", "Boss Blue Warp", "Romani's Game", "Day 1 Grave Tablet", "Imprisoned Monkey", "Baby Goron", "Baby Zoras", "Ikana King", "Swamp Music Statue"
];
var SongNames = {
	"song_of_healing": "Healing", 
	"eponas_song": "Eponas", 
	"song_of_storms": "Storms", 
	"sonata": "Sonata", 
	"lullaby": "Lullaby", 
	"nwbn": "NWBN", 
	"elegy": "Elegy", 
	"oath": "Oath"
};

var inputs = [
	"x", "bow", "fir", "ice", "lig", "moo", "lan", "swa", "mou", "oce", "bom", "bea", "roo", "spe", "pow", "pic", "len", "hoo", "gfs", "let", "pen", "bot", "gol", "pos", "all", "bla", "sto", "gre", "dek", "kea", "bre", "bun", "don", "sce", "gor", "rom", "cir", "kaf", "cou", "tru", "zor", "kam", "gib", "gar", "cap", "gia", "fie", "swo", "mir", "mag", "wal", "soh", "epo", "sos", "son", "lul", "nov", "ele", "oat"
];

var hintInputs = ["swa", "oce", "ali", "cre", "but", "boa", "dam", "rac", "fis", "ban", "bea", "gos", "sea", "sm2", "mid", "pos", "ka1", "ka2", "ka3", "poe", "hun", "lef", "pam", "gor", "gro", "dog", "iro", "tar", "sar", "bom", "hon", "pla", "bro", "dar", "lig", "ice", "fir", "hoo", "kam", "inv", "spi"];

var hintIndexes = ["Swamp Spider House Reward", "Ocean Spider House Day 1 Reward", "Aliens Defense", "Cremia", "Butler", "Boat Archery", "Dampe Digging", "Goron Race", "Fisherman Game", "Bank Reward #3", "Beaver Race #1", "Gossip Stones", "Seahorses", "Mountain Smithy Day 2", "Midnight Meeting", "Postman's Freedom Reward", "Kafei", "Curiosity Shop Man #1", "Curiosity Shop Man #2", "Poe Hut", "Hungry Goron", "Well Left Path Chest", "Pamela's Father", "Gorman", "Grog", "Dog Race", "Iron Knuckle Chest", "Town Archery #1", "Swamp Archery #1", "Bombers' Hide and Seek", "Honey and Darling Any Day", "Deku Playground Any Day", "Gorman Bros Race", "Woodfall Dark Room", "Light Arrow Chest", "Ice Arrow Chest", "Fire Arrow Chest", "Hookshot Chest", "Kamaro", "Invisible Soldier", "Ocean Spider House Chest"];

var hintStrings1 = ["It appears ", "They say ", "Apparently ", "I hear ", "It seems "];
var hintStrings2 = [" holds ", " brings ", " possesses ", " conceals ", " yields ", " leads to "];

var alwaysHints = ["Swamp Spider House Reward", "Ocean Spider House Day 1 Reward", "Aliens Defense", "Cremia", "Butler", "Boat Archery", "Dampe Digging"];

var Hinted = {};
var hintedInput = "";
var thisIsHinted = false;

var Area = []; // used for woth/barrens
var woth1 = "unknown";
var woth2 = "unknown";
var woth3 = "unknown";
var woth4 = "unknown";
var woth5 = "unknown";
var AreaWotHAge = new Array(35).fill(0);
var WotHColors = ["", "9cc4d9", "white", "b19cd9", "d09cd9", "cyan"];

var MarkedWotHItemArrow = null;
var ManualWotHItems = {};
var ManualInLogicItems = {};
var ManualOutOfLogicItems = {};
var ManualNotWotHItems = {};
var ManualWotHMinorItems = {};
var ManualWotHItemLocked = {};
var ManualWotHItemPutInLogic = {};

var backUp = [];
var toFocus = null;
var overrideFocus = false;

var parent = document.getElementById("normalColumn1");
for (var i = 0; i < Locations.length; i++) {
	if (i == AreaIndexes[5]) { parent = document.getElementById("normalColumn2"); }
	else if (i == AreaIndexes[8]) { parent = document.getElementById("normalColumn3"); }
	else if (i == AreaIndexes[14]) { parent = document.getElementById("normalColumn4"); }
	else if (i == AreaIndexes[19]) { parent = document.getElementById("normalColumn5"); }
	else if (i == AreaIndexes[25]) { parent = document.getElementById("normalColumn6"); }
	else if (i == AreaIndexes[32]) { parent = document.getElementById("normalColumn7"); }
	else if (i == AreaIndexes[35]) { parent = document.getElementById("normalColumn8"); }
	else if (i == AreaIndexes[36]) { parent = document.getElementById("songdiv"); }
	
	for(var j = 1; j < AreaIndexes.length; j++) {
		if(i == AreaIndexes[j]) {
			var elem = document.createElement("small"); 
			elem.innerHTML = AreaNames[j+1]; 
			elem.className = "area_name"; 
			if(j == 32) {
				elem.id = "title_wft";
				elem.className = "area_titles";
			}
			else if(j == 33) {
				elem.id = "title_sht";
				elem.className = "area_titles";
			}
			else if(j == 34) {
				elem.id = "title_gbt";
				elem.className = "area_titles";
			}
			else if(j == 35) {
				elem.id = "title_stt";
				elem.className = "area_titles";
			}
			else if(j == 36) {
				elem.id = "song_title";
				elem.className = "area_titles";
			}
			
			if(j < 32)
				background = "url('./images/areas/"+AreaImages[j]+"')";
			else
				background = "";
			
			parent.appendChild(elem); 
			parent.appendChild(document.createElement("br")); 
			break;
		}
	}
	
	if(i == AreaIndexes[35]+16) {
		var elem = document.createElement("small"); 
		elem.innerHTML = "ISTT"; 
		elem.className = "area_name";
		elem.id = "title_stt";
		elem.className = "area_titles";
		
		background = "";
		
		parent.appendChild(elem); 
		parent.appendChild(document.createElement("br")); 
	}
	
	var elem = document.createElement("input"); 
	elem.id = Locations[i]; 
	if(parent != document.getElementById("normalColumn7") && parent != document.getElementById("normalColumn8") && parent != document.getElementById("songdiv"))
		elem.className = "picture_input"; 
	else
		elem.className = "other_input"; 
	elem.style.backgroundImage = background;
	parent.appendChild(elem);

	var elem = document.createElement("small"); 
	elem.id = "text_" + Locations[i]; 
	elem.className = "check_text"; 
	if(SongItems.indexOf(Locations[i]) < 0)
		elem.onmousedown = junk; 
	else
		elem.onmousedown = function() {clickSummary(this);};
	elem.innerHTML = Names[i]; 
	
	if(elem.id == "text_Inn Reservation")
		elem.title = "Day 1: 1:50pm-4:10pm";
	if(elem.id == "text_Honey and Darling Any Day")
		elem.title = "Day 1-2: 6:00am-10:00pm\nDay 3: 6:00am-6:00pm\n1: Bowling, 2: Basketball, 3: Archery";
	if(elem.id == "text_Treasure Chest Game Goron")
		elem.title = "Day 1-2: 6:00am-10:00pm\nDay 3: 6:00am-6:00pm";
	if(elem.id == "text_Swamp Archery #1")
		elem.title = "Day 1-3: 6:00am-10:00pm";
	if(elem.id == "text_Town Archery #1")
		elem.title = "Day 1-3: 6:00am-10:00pm\nNeed 40 points";
	
	if(elem.id == "text_Grandma Short Story" || elem.id == "text_Grandma Long Story")
		elem.title = "Day 1-2: 6:00am-6:00pm\nStock Pot open from 8:00am-8:30pm";
	if(elem.id == "text_Mayor")
		elem.title = "Day 1-2: 10:00am-8:00pm\nDay 3: 10:00am-6:00pm";
	if(elem.id == "text_Madame Aroma in Office")
		elem.title = "Day 1-2: 10:00am-8:00pm";
	if(elem.id == "text_Gorman")
		elem.title = "Night 1-2: 10:00pm-5:00am";
	if(elem.id == "text_Midnight Meeting")
		elem.title = "Set Meeting: Day 1: 2:20pm-9:03pm\nAttend Meeting: Night 1: 12:00am-5:59am\n\n(She walks to front door at 7:50pm-8:30pm. Kicked out at 8:30pm.\nShe walks to staff room from 8:30-9:03pm)";
	if(elem.id == "text_Toilet Hand")
		elem.title = "Day 1-3: 12:00am-5:59am";
	if(elem.id == "text_Madame Aroma in Bar")
		elem.title = "Night 3: 6:00pm-9:00pm (without Romani Mask)\nNight 3: 10:00pm-5:59am (with Romani Mask)";
	if(elem.id == "text_Milk Bar Milk" || elem.id == "text_Milk Bar Chateau")
		elem.title = "Night 1-2: 10:00pm-5:00am\nNight 3: 6:00pm-9:00pm, 10:00pm-5:59am";
	if(elem.id == "text_Inn Staff Room Chest")
		elem.title = "Night 3: 6:00pm-5:59am";
	if(elem.id == "text_South Clock Town Final Day Chest")
		elem.title = "Day 3: 6:00am-5:59am";
	if(elem.id == "text_Old Lady")
		elem.title = "Night 1: 12:20am";
	if(elem.id == "text_Deku Playground Any Day")
		elem.title = "Day 1-3: 6:00am-11:30pm";
	if(elem.id == "text_Swordsman's School")
		elem.title = "Day 1-2: any time\nDay 3: 6:00am-11:00pm";
	if(elem.id == "text_Postman's Game")
		elem.title = "Day 1: 3:00pm-12:00am\nDay 2: 3:00pm-12:00am (if letter to kafei delivered)";
	if(elem.id == "text_Rosa Sisters")
		elem.title = "Night 1-2: 6:00pm-5:59am";
	if(elem.id == "text_Big Bomb Bag Purchase")
		elem.title = "If saved old lady:\nAny time after 12:30 am on Night 1\n\nIf didn't save old lady:\nNight 3: 10:00pm-5:00am (costs 100r)";
	if(elem.id == "text_All-Night Mask Purchase")
		elem.title = "Night 3: 10:00pm-5:00am\nMust have saved old lady this cycle";
	if(elem.id == "text_Postman's Freedom Reward")
		elem.title = "Night 3: 6:00pm-5:00am (takes 2 hours to deliver)";
	if(elem.id == "text_Guru Guru")
		elem.title = "Night 1-2: 6:00pm-5:59am";
	if(elem.id == "text_Kafei")
		elem.title = "Day 2: 4:00pm-10:00pm (letter must be delivered before 11:20 am Day 2)\nMust not have saved old lady this cycle";
	if(elem.id == "text_Curiosity Shop Man #1" || elem.id == "text_Curiosity Shop Man #2")
		elem.title = "Day 3: 1:00pm-10:00pm (letter must be delivered before 11:20 am Day 2)\nMust not have saved old lady this cycle\nMust have gotten item from Kafei this cycle";
	
	if(elem.id == "text_Kamaro")
		elem.title = "Night 1-3: 12:00am-5:59am";
	
	if(elem.id == "text_Dog Race" || elem.id == "text_Grog" || elem.id == "text_Doggy Racetrack Roof Chest")
		elem.title = "Day 1-3: 6:00am-8:00pm";
	if(elem.id == "text_Gorman Bros Race")
		elem.title = "Day 1-3: 6:00am-6:00pm";
	if(elem.id == "text_Gorman Bros Milk Purchase")
		elem.title = "Day 1-3: 6:00am-6:00pm";
	if(elem.id == "text_Romani's Game")
		elem.title = "Day 1: 6:00am-6:00pm\nAliens: Night 1: 2:30am-5:10pm (3:50am lose)\nCremia: Night 2: 6:00pm-7:00pm\n(sets tod to 7:45pm)";
	if(elem.id == "text_Aliens Defense")
		elem.title = "Night 1: 2:30am-5:10pm (3:50am lose)";
	if(elem.id == "text_Cremia")
		elem.title = "Night 2: 6:00pm-7:00pm\n(sets time to 7:45pm)";
	
	if(elem.id == "text_Mystery Woods Grotto")
		elem.title = "Day 2: 6:00am-5:59am";
	
	if(elem.id == "text_Ocean Spider House Day 1 Reward")
		elem.title = "Day 1: 6:00am-5:59am";
	if(elem.id == "text_Fisherman Game")
		elem.title = "Boat ride available: 7:00am-6:00pm\nMinigame open: any time except 4-7am or 4-7pm";
	if(elem.id == "text_Zora Hall Stage Lights" || elem.id == "text_Mikau")
		elem.title = "Cannot do after beating GBT";
	
	if(elem.id == "text_Day 1 Grave Bats" || elem.id == "text_Day 1 Grave Tablet")
		elem.title = "Night 1-2: 6:00pm-5:59am";
	if(elem.id == "text_Iron Knuckle Chest")
		elem.title = "Night 1-2: 6:00pm-5:59am";
	if(elem.id == "text_Dampe Digging")
		elem.title = "Night 3: 6:00pm-5:59am";
	
	if(elem.id == "text_Mountain Smithy Day 1")
		elem.title = "Must purchase on Day 1, then receive Day 2";
	if(elem.id == "text_Mountain Smithy Day 2")
		elem.title = "Must give Gold Dust on Day 2, then receive Day 3";
	
	if(elem.id == "text_Pictograph Contest Winner" || elem.id == "text_Koume")
		elem.title = "Cannot do after beating Woodfall";
	
	if(elem.id == "text_Lens Cave Rock Chest" || elem.id == "text_Lens Cave Invisible Chest" || elem.id == "text_Lens of Truth Chest")
		elem.title = "Cannot do after beating SHT";
	
	if(elem.id == "text_Zora Hall Stage Lights" || elem.id == "text_Mikau" || elem.id == "text_Evan")
		elem.title = "Cannot do after beating GBT";
	
	if(elem.id == "text_Poe Hut" || elem.id == "text_Pamela's Father")
		elem.title = "Cannot do after beating STT";
	
	parent.appendChild(elem);
	
	var elem = document.createElement("br"); 
	elem.id = "br_" + Locations[i]; 
	parent.appendChild(elem);
}

// hide logically useless items from check summary list
document.getElementById("pendant_of_memories_location").style.display = "none";
document.getElementById("pendant_of_memories_location_br").style.display = "none";
document.getElementById("circusleaders_mask_location").style.display = "none";
document.getElementById("circusleaders_mask_location_br").style.display = "none";
document.getElementById("dongero_mask_location").style.display = "none";
document.getElementById("dongero_mask_location_br").style.display = "none";
document.getElementById("greatfairy_mask_location").style.display = "none";
document.getElementById("greatfairy_mask_location_br").style.display = "none";

var hintbox = document.getElementById("hintInput");
if (document.getElementById("settings_option").value == "S3")
	hintbox.innerHTML = "swa \noce \nali \ncre \nbut \nboa \ndam \n";

var Logic = {};
var Game = {};
var CouldHave={};
var Location_Logic = {};
var Location_Access = {};
var Location_Obtain = {};
var Location_Could_Obtain={};
var Location_Could_Peek={};
var lastCheck = ["start"];
var forcedDisplay = new Array(1024).fill(false);

for (var i = 0; i < Items.length; i++) {
	Logic[Items[i]] = false;
	Game[Items[i]] = false;
	CouldHave[Items[i]] = false;
	Known[Items[i]] = false;
}

var simActive = false;
var SpoilerLines = [];
var SpoilerLocToItem = {};
var SpoilerHintDict = {};
var simWothsEntered = {};
var simWothCounter = 1;
var simBarrensEntered = {};
var simBarrenCounter = 1;

for (var i = 0; i < Locations.length; i++) {
	Location_Logic[Locations[i]] = false;
	Location_Access[Locations[i]] = false;
	Check[Locations[i]] = "unknown";
	backUp.push(document.getElementById("text_" + Locations[i]).innerHTML);
}

Game.sword1_img = "./images/Kokiri Sword.png";
Game.sword2_img = "./images/Razor Sword.png";
Game.sword3_img = "./images/Gilded Sword.png";
Game.shield1_img = "./images/Hero's Shield.png";
Game.shield2_img = "./images/Mirror Shield.png";
Game.wallet1_img = "./images/Adult's Wallet.png";
Game.wallet2_img = "./images/Giant's Wallet.png";
Game.magic1_img = "./images/Magic.png";
Game.magic2_img = "./images/Magic2.png";

Game.checks_remaining = 0;
Game.logically_accessible = 0;

var modal = document.getElementById("myModal");
var span = document.getElementsByClassName("close")[0];

span.onclick = function() {
  modal.style.display = "none";
}
window.onclick = function(event) {
  if (event.target == modal) {
	modal.style.display = "none";
  }
}

window.onbeforeunload = popup;
function popup() {
  return '';
}

setInterval(Update,250); 
Update();Update();Update();
