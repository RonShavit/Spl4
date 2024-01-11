/*
S3 Toggled tricks:

-Take Damage
-Scarecrows Song
-Lensless Chests
-Exit Ocean Spider House without Goron
-Run Through Poisoned Water
-Climb Stone Tower with One Transformation
-Lensless Walking
-WFT 2nd Floor Skip
-Deku Palace Bean Skip
-Pinnacle Rock without Seahorse
-Lensless Walls/Ceilings
*/

function update_item_logic() {
	// Assumes you start with Kokiri Sword, Hero's Shield, Ocarina of Time, Song of Time, Song of Soaring
	
	// Progressive Swords
	if(Known.sword1 == true) {Logic.sword1 = Location_Logic[ItemLocation.sword1];} 
	if(Known.sword2 == true) {Logic.sword2 = Location_Logic[ItemLocation.sword2];}
	Logic.razor_sword = Logic.sword1 || Logic.sword2;
	Logic.gilded_sword = Logic.sword1 && Logic.sword2;
	
	// Progressive Magic Upgrades
	if(Known.magic1 == true) {Logic.magic1 = Location_Logic[ItemLocation.magic1];} 
	if(Known.magic2 == true) {Logic.magic2 = Location_Logic[ItemLocation.magic2];}
	Logic.magic = Logic.magic1 || Logic.magic2;
	Logic.double_magic = Logic.magic1 && Logic.magic2;
	
	// Progressive Wallets
	if(Known.wallet1 == true) {Logic.wallet1 = Location_Logic[ItemLocation.wallet1];} 
	if(Known.wallet2 == true) {Logic.wallet2 = Location_Logic[ItemLocation.wallet2];}
	Logic.adults_wallet = Logic.wallet1 || Logic.wallet2;
	Logic.giants_wallet = Logic.wallet1 && Logic.wallet2;
	
	// Progressive Bows
	if(Known.bow1 == true) {Logic.bow1 = Location_Logic[ItemLocation.bow1];} 
	if(Known.bow2 == true) {Logic.bow2 = Location_Logic[ItemLocation.bow2];}
	if(Known.bow3 == true) {Logic.bow3 = Location_Logic[ItemLocation.bow3];}
	Logic.bow = Logic.bow1 || Logic.bow2 || Logic.bow3;
	
	// Progressive Bomb Bags
	if(Known.bomb1 == true) {Logic.bomb1 = Location_Logic[ItemLocation.bomb1];} 
	if(Known.bomb2 == true) {Logic.bomb2 = Location_Logic[ItemLocation.bomb2];}
	if(Known.bomb3 == true) {Logic.bomb3 = Location_Logic[ItemLocation.bomb3];}
	Logic.bomb = Logic.bomb1 || Logic.bomb2 || Logic.bomb3;
	
	// Bottles
	if(Known.bottle1 == true) {Logic.bottle1 = Location_Logic[ItemLocation.bottle1];}
	if(Known.bottle2 == true) {Logic.bottle2 = Location_Logic[ItemLocation.bottle2];}
	if(Known.bottle3 == true) {Logic.bottle3 = Location_Logic[ItemLocation.bottle3];}
	if(Known.bottle4 == true) {Logic.bottle4 = Location_Logic[ItemLocation.bottle4];}
	if(Known.bottle5 == true) {Logic.bottle5 = Location_Logic[ItemLocation.bottle5];}
	if(Known.bottle_gold_dust == true) {Logic.bottle_gold_dust = Location_Logic[ItemLocation.bottle_gold_dust];} else{Logic.bottle_gold_dust = false;}
	Logic.any_bottle = Logic.bottle1 || Logic.bottle2 || Logic.bottle3 || Logic.bottle4 || Logic.bottle5 || Logic.bottle_gold_dust;
	
	// Magic beans
	if(Known.magic_bean1 == true) {Logic.magic_bean1 = Location_Logic[ItemLocation.magic_bean1];}
	if(Known.magic_bean2 == true) {Logic.magic_bean2 = Location_Logic[ItemLocation.magic_bean2];}
	Logic.magic_bean = Logic.magic_bean1 || Logic.magic_bean2;
	
	// Singleton Items
	if(Known.fire_arrow == true) {Logic.fire_arrow = Location_Logic[ItemLocation.fire_arrow];} else{Logic.fire_arrow = false;}
	if(Known.ice_arrow == true) {Logic.ice_arrow = Location_Logic[ItemLocation.ice_arrow];} else{Logic.ice_arrow = false;}
	if(Known.light_arrow == true) {Logic.light_arrow = Location_Logic[ItemLocation.light_arrow];} else{Logic.light_arrow = false;}
	if(Known.powder_keg == true) {Logic.powder_keg = Location_Logic[ItemLocation.powder_keg];} else{Logic.powder_keg = false;}
	if(Known.pictobox == true) {Logic.pictobox = Location_Logic[ItemLocation.pictobox];} else{Logic.pictobox = false;}
	if(Known.lens == true) {Logic.lens = Location_Logic[ItemLocation.lens];} else{Logic.lens = false;}
	if(Known.hookshot == true) {Logic.hookshot = Location_Logic[ItemLocation.hookshot];} else{Logic.hookshot = false;}
	if(Known.great_fairy_sword == true) {Logic.great_fairy_sword = Location_Logic[ItemLocation.great_fairy_sword];} else{Logic.great_fairy_sword = false;}
	
	if(Known.postmans_hat == true) {Logic.postmans_hat = Location_Logic[ItemLocation.postmans_hat];} else{Logic.postmans_hat = false;}
	if(Known.allnight_mask == true) {Logic.allnight_mask = Location_Logic[ItemLocation.allnight_mask];} else{Logic.allnight_mask = false;}
	if(Known.blast_mask == true) {Logic.blast_mask = Location_Logic[ItemLocation.blast_mask];} else{Logic.blast_mask = false;}
	if(Known.stone_mask == true) {Logic.stone_mask = Location_Logic[ItemLocation.stone_mask];} else{Logic.stone_mask = false;} // logically useless
	if(Known.greatfairy_mask == true) {Logic.greatfairy_mask = Location_Logic[ItemLocation.greatfairy_mask];} else{Logic.greatfairy_mask = false;} // logically useless
	if(Known.deku_mask == true) {Logic.deku_mask = Location_Logic[ItemLocation.deku_mask];} else{Logic.deku_mask = false;}
	if(Known.keaton_mask == true) {Logic.keaton_mask = Location_Logic[ItemLocation.keaton_mask];} else{Logic.keaton_mask = false;}
	if(Known.bremen_mask == true) {Logic.bremen_mask = Location_Logic[ItemLocation.bremen_mask];} else{Logic.bremen_mask = false;}
	if(Known.bunny_hood == true) {Logic.bunny_hood = Location_Logic[ItemLocation.bunny_hood];} else{Logic.bunny_hood = false;}
	if(Known.dongero_mask == true) {Logic.dongero_mask = Location_Logic[ItemLocation.dongero_mask];} else{Logic.dongero_mask = false;}
	if(Known.mask_of_scents == true) {Logic.mask_of_scents = Location_Logic[ItemLocation.mask_of_scents];} else{Logic.mask_of_scents = false;} // logically useless
	if(Known.goron_mask == true) {Logic.goron_mask = Location_Logic[ItemLocation.goron_mask];} else{Logic.goron_mask = false;}
	if(Known.romani_mask == true) {Logic.romani_mask = Location_Logic[ItemLocation.romani_mask];} else{Logic.romani_mask = false;}
	if(Known.circusleaders_mask == true) {Logic.circusleaders_mask = Location_Logic[ItemLocation.circusleaders_mask];} else{Logic.circusleaders_mask = false;} // logically useless
	if(Known.kafei_mask == true) {Logic.kafei_mask = Location_Logic[ItemLocation.kafei_mask];} else{Logic.kafei_mask = false;}
	if(Known.couples_mask == true) {Logic.couples_mask = Location_Logic[ItemLocation.couples_mask];} else{Logic.couples_mask = false;}
	if(Known.mask_of_truth == true) {Logic.mask_of_truth = Location_Logic[ItemLocation.mask_of_truth];} else{Logic.mask_of_truth = false;}
	if(Known.zora_mask == true) {Logic.zora_mask = Location_Logic[ItemLocation.zora_mask];} else{Logic.zora_mask = false;}
	if(Known.kamaro_mask == true) {Logic.kamaro_mask = Location_Logic[ItemLocation.kamaro_mask];} else{Logic.kamaro_mask = false;}
	if(Known.gibdo_mask == true) {Logic.gibdo_mask = Location_Logic[ItemLocation.gibdo_mask];} else{Logic.gibdo_mask = false;}
	if(Known.garo_mask == true) {Logic.garo_mask = Location_Logic[ItemLocation.garo_mask];} else{Logic.garo_mask = false;}
	if(Known.captains_hat == true) {Logic.captains_hat = Location_Logic[ItemLocation.captains_hat];} else{Logic.captains_hat = false;}
	if(Known.giants_mask == true) {Logic.giants_mask = Location_Logic[ItemLocation.giants_mask];} else{Logic.giants_mask = false;}
	if(Known.fiercedeity_mask == true) {Logic.fiercedeity_mask = Location_Logic[ItemLocation.fiercedeity_mask];} else{Logic.fiercedeity_mask = false;} // logically useless
	
	if(Known.moons_tear == true) {Logic.moons_tear = Location_Logic[ItemLocation.moons_tear];} else{Logic.moons_tear = false;}
	if(Known.land_title_deed == true) {Logic.land_title_deed = Location_Logic[ItemLocation.land_title_deed];} else{Logic.land_title_deed = false;}
	if(Known.swamp_title_deed == true) {Logic.swamp_title_deed = Location_Logic[ItemLocation.swamp_title_deed];} else{Logic.swamp_title_deed = false;}
	if(Known.mountain_title_deed == true) {Logic.mountain_title_deed = Location_Logic[ItemLocation.mountain_title_deed];} else{Logic.mountain_title_deed = false;}
	if(Known.ocean_title_deed == true) {Logic.ocean_title_deed = Location_Logic[ItemLocation.ocean_title_deed];} else{Logic.ocean_title_deed = false;}
	
	if(Known.room_key == true) {Logic.room_key = Location_Logic[ItemLocation.room_key];} else{Logic.room_key = false;}
	if(Known.special_delivery == true) {Logic.special_delivery = Location_Logic[ItemLocation.special_delivery];} else{Logic.special_delivery = false;}
	if(Known.letter_to_kafei == true) {Logic.letter_to_kafei = Location_Logic[ItemLocation.letter_to_kafei];} else{Logic.letter_to_kafei = false;}
	if(Known.pendant_of_memories == true) {Logic.pendant_of_memories = Location_Logic[ItemLocation.pendant_of_memories];} else{Logic.pendant_of_memories = false;}
	
	if(Known.eponas_song == true) {Logic.eponas_song = Location_Logic[ItemLocation.eponas_song];} else{Logic.eponas_song = false;}
	if(Known.song_of_storms == true) {Logic.song_of_storms = Location_Logic[ItemLocation.song_of_storms];} else{Logic.song_of_storms = false;}
	if(Known.song_of_healing == true) {Logic.song_of_healing = Location_Logic[ItemLocation.song_of_healing];} else{Logic.song_of_healing = false;}
	if(Known.sonata == true) {Logic.sonata = Location_Logic[ItemLocation.sonata];} else{Logic.sonata = false;}
	if(Known.lullaby == true) {Logic.lullaby = Location_Logic[ItemLocation.lullaby];} else{Logic.lullaby = false;}
	if(Known.nwbn == true) {Logic.nwbn = Location_Logic[ItemLocation.nwbn];} else{Logic.nwbn = false;}
	if(Known.elegy == true) {Logic.elegy = Location_Logic[ItemLocation.elegy];} else{Logic.elegy = false;}
	if(Known.oath == true) {Logic.oath = Location_Logic[ItemLocation.oath];} else{Logic.oath = false;}
	
	if(Known.mirror_shield == true) {Logic.mirror_shield = Location_Logic[ItemLocation.mirror_shield];} else{Logic.mirror_shield = false;}
	
	Game.razor_sword = Game.sword1 || Game.sword2;
	Game.gilded_sword = Game.sword1 && Game.sword2;
	
	Game.adults_wallet = Game.wallet1 || Game.wallet2;
	Game.giants_wallet = Game.wallet1 && Game.wallet2;
	
	Game.magic = Game.magic1 || Game.magic2;
	Game.double_magic = Game.magic1 && Game.magic2;
	
	Game.bow = Game.bow1 || Game.bow2 || Game.bow3;
	Game.bomb = Game.bomb1 || Game.bomb2 || Game.bomb3;
	Game.magic_bean = Game.magic_bean1 || Game.magic_bean2;
	Game.any_bottle = Game.bottle1 || Game.bottle2 || Game.bottle3 || Game.bottle4 || Game.bottle5 || Game.bottle_gold_dust;
	
	for (var q = 0; q < 20; q++) {
		for (var i = 0; i < Items.length; i++) {
			if(Location_Could_Obtain[ItemLocation[Items[i]]] || Game[Items[i]])
				CouldHave[Items[i]] = true;
			else
				CouldHave[Items[i]] = false;
		}
	}
	
	CouldHave.razor_sword = CouldHave.sword1 || CouldHave.sword2;
	CouldHave.gilded_sword = CouldHave.sword1 && CouldHave.sword2;
	
	CouldHave.adults_wallet = CouldHave.wallet1 || CouldHave.wallet2;
	CouldHave.giants_wallet = CouldHave.wallet1 && CouldHave.wallet2;
	
	CouldHave.magic = CouldHave.magic1 || CouldHave.magic2;
	CouldHave.double_magic = CouldHave.magic1 && CouldHave.magic2;
	
	CouldHave.bow = CouldHave.bow1 || CouldHave.bow2 || CouldHave.bow3;
	CouldHave.bomb = CouldHave.bomb1 || CouldHave.bomb2 || CouldHave.bomb3;
	CouldHave.magic_bean = CouldHave.magic_bean1 || CouldHave.magic_bean2;
	CouldHave.any_bottle = CouldHave.bottle1 || CouldHave.bottle2 || CouldHave.bottle3 || CouldHave.bottle4 || CouldHave.bottle5 || CouldHave.bottle_gold_dust;
	
	
	// Misc Shortcuts
	Logic.explosive = Logic.bomb || Logic.blast_mask;
	Logic.night_inn_access = Logic.deku_mask || Logic.room_key;
	Logic.ranch_day_1_access = Logic.goron_mask && Logic.powder_keg;
	Logic.water_for_magic_bean = Logic.any_bottle || Logic.song_of_storms;
	Logic.shoot_fire_arrow = Logic.bow && Logic.fire_arrow && Logic.magic;
	Logic.shoot_ice_arrow = Logic.bow && Logic.ice_arrow && Logic.magic;
	Logic.shoot_light_arrow = Logic.bow && Logic.light_arrow && Logic.magic;
	
	// Swamp area shortcuts
	Logic.cross_poisoned_water = Logic.deku_mask || Logic.zora_mask;
	Logic.poison_swamp_access = Logic.cross_poisoned_water && (Logic.hookshot || Logic.bow || Logic.pictobox || Logic.zora_mask || Logic.any_bottle);
	Logic.limitless_magic_beans = Logic.poison_swamp_access && Logic.deku_mask;
	Logic.any_magic_bean = (Logic.poison_swamp_access && Logic.deku_mask) || Logic.magic_bean; 
	
	Logic.wft_access = Logic.deku_mask && Logic.sonata && Logic.poison_swamp_access;
	Logic.woodfall_clear = Logic.wft_access && Logic.bow; 
	
	// Mountain area shortcuts
	Logic.north_access = Logic.bow && (Logic.explosive || Logic.shoot_fire_arrow || Logic.goron_mask);
	Logic.hot_spring_water = Logic.any_bottle && ((Logic.north_access && ((Logic.lens && Logic.goron_mask && Logic.magic) || Logic.shoot_fire_arrow || Logic.snowhead_clear)) || (Logic.ikana_canyon_access && Logic.gibdo_mask && Logic.any_blue_potion && (Logic.zora_mask || Logic.explosive)));
	
	Logic.sht_access = Logic.north_access && Logic.goron_mask && Logic.lullaby && Logic.magic;
	Logic.snowhead_clear = Logic.sht_access && Logic.shoot_fire_arrow;
	
	// Great Bay area shortcuts
	Logic.west_access = Logic.eponas_song;
	Logic.ocean_skulltulas = Logic.explosive && Logic.west_access && Logic.hookshot;
	Logic.pirates_fortress_access = Logic.west_access && Logic.zora_mask;
	Logic.pinnacle_rock_access = Logic.zora_mask;
	Logic.seahorse = Logic.pictobox && Logic.west_access && Logic.pirates_fortress_access;
	Logic.gbt_access = Logic.pirates_fortress_access && Logic.nwbn && Logic.hookshot;
	Logic.great_bay_clear = Logic.gbt_access && Logic.shoot_fire_arrow && Logic.shoot_ice_arrow; 

	// Ikana area shortcuts
	Logic.ikana_graveyard_access = Logic.eponas_song;
	Logic.east_access = Logic.hookshot && Logic.ikana_graveyard_access && (Logic.garo_mask || Logic.gibdo_mask);
	Logic.ikana_canyon_access = Logic.east_access && Logic.shoot_ice_arrow;
	Logic.stt_access = Logic.ikana_canyon_access && Logic.hookshot && Logic.elegy && (Logic.goron_mask || Logic.zora_mask);
	Logic.istt_access = Logic.stt_access && Logic.shoot_light_arrow;
	Logic.ikana_clear = Logic.istt_access && Logic.deku_mask && (Logic.giants_mask || Logic.fiercedeity_mask);
	
	// Bottle shortcuts
	Logic.any_blue_potion = Logic.any_bottle && (Logic.adults_wallet || Logic.mask_of_scents);
	Logic.any_milk = Logic.any_bottle; // gorman bros purchase requires nothing but bottle
	Logic.zora_egg = Logic.any_bottle && Logic.pirates_fortress_access && Logic.hookshot && Logic.pinnacle_rock_access && ((Logic.deku_mask && Logic.magic) || Logic.bow);
	Logic.big_poe = Logic.any_bottle && Logic.ikana_canyon_access && Logic.gibdo_mask && Logic.bomb && (Logic.shoot_light_arrow || (Logic.limitless_magic_beans && (Logic.bow || Logic.zora_mask)));
	
	
	// Misc Shortcuts
	Game.explosive = Game.bomb || Game.blast_mask;
	Game.night_inn_access = Game.deku_mask || Game.room_key || Game.explosive || Game.bow || Game.hookshot || Game.zora_mask;
	Game.ranch_day_1_access = Game.goron_mask && Game.powder_keg;
	Game.water_for_magic_bean = Game.any_bottle || Game.song_of_storms;
	Game.shoot_fire_arrow = Game.bow && Game.fire_arrow && Game.magic;
	Game.shoot_ice_arrow = Game.bow && Game.ice_arrow && Game.magic;
	Game.shoot_light_arrow = Game.bow && Game.light_arrow && Game.magic;
	
	// Swamp area shortcuts
	Game.cross_poisoned_water = Game.deku_mask || Game.zora_mask || Game.goron_mask;
	Game.poison_swamp_access = Game.cross_poisoned_water && (Game.hookshot || Game.bow || Game.pictobox || Game.zora_mask || Game.any_bottle);
	Game.limitless_magic_beans = Game.poison_swamp_access && Game.deku_mask;
	Game.any_magic_bean = (Game.poison_swamp_access && Game.deku_mask) || Game.magic_bean; 
	
	Game.wft_access = Game.deku_mask && Game.sonata && Game.poison_swamp_access;
	Game.woodfall_clear = Game.wft_access && Game.bow; 
	
	// Mountain area shortcuts
	Game.north_access = Game.bow && (Game.explosive || Game.shoot_fire_arrow || Game.goron_mask);
	Game.hot_spring_water = Game.north_access && Game.any_bottle && (Game.goron_mask || Game.shoot_fire_arrow || Game.snowhead_clear || (Game.ikana_canyon_access && Game.gibdo_mask && Game.any_blue_potion));
	
	Game.sht_access = Game.north_access && Game.goron_mask && Game.lullaby;
	Game.snowhead_clear = Game.sht_access && Game.shoot_fire_arrow;
	
	// Great Bay area shortcuts
	Game.west_access = Game.eponas_song;
	Game.ocean_skulltulas = Game.explosive && Game.west_access && Game.hookshot;
	Game.pirates_fortress_access = Game.west_access && Game.zora_mask;
	Game.pinnacle_rock_access = Game.zora_mask;
	Game.seahorse = Game.pictobox && Game.west_access && Game.pirates_fortress_access;
	Game.gbt_access = Game.pirates_fortress_access && Game.nwbn && Game.hookshot;
	Game.great_bay_clear = Game.gbt_access && Game.shoot_ice_arrow; 

	// Ikana area shortcuts
	Game.ikana_graveyard_access = Game.eponas_song;
	Game.east_access = Game.hookshot && Game.ikana_graveyard_access && (Game.garo_mask || Game.gibdo_mask);
	Game.ikana_canyon_access = Game.east_access;
	Game.stt_access = Game.ikana_canyon_access && Game.hookshot && Game.elegy && (Game.goron_mask || Game.zora_mask);
	Game.istt_access = Game.stt_access && Game.shoot_light_arrow;
	Game.ikana_clear = Game.istt_access;
	
	// Bottle shortcuts
	Game.any_blue_potion = Game.any_bottle && (Game.adults_wallet || Game.mask_of_scents);
	Game.any_milk = Game.any_bottle; // gorman bros purchase requires nothing but bottle
	Game.zora_egg = Game.any_bottle && Game.pirates_fortress_access && Game.hookshot && Game.pinnacle_rock_access && ((Game.deku_mask && Game.magic) || Game.bow);
	Game.big_poe = Game.any_bottle && Game.ikana_canyon_access && Game.gibdo_mask && Game.bomb && (Game.shoot_light_arrow || (Game.limitless_magic_beans && (Game.bow || Game.zora_mask)));
	
	
	// Misc Shortcuts
	CouldHave.explosive = CouldHave.bomb || CouldHave.blast_mask;
	CouldHave.night_inn_access = CouldHave.deku_mask || CouldHave.room_key || CouldHave.explosive || CouldHave.bow || CouldHave.hookshot || CouldHave.zora_mask;
	CouldHave.ranch_day_1_access = CouldHave.goron_mask && CouldHave.powder_keg;
	CouldHave.water_for_magic_bean = CouldHave.any_bottle || CouldHave.song_of_storms;
	CouldHave.shoot_fire_arrow = CouldHave.bow && CouldHave.fire_arrow && CouldHave.magic;
	CouldHave.shoot_ice_arrow = CouldHave.bow && CouldHave.ice_arrow && CouldHave.magic;
	CouldHave.shoot_light_arrow = CouldHave.bow && CouldHave.light_arrow && CouldHave.magic;
	
	// Swamp area shortcuts
	CouldHave.cross_poisoned_water = CouldHave.deku_mask || CouldHave.zora_mask || CouldHave.goron_mask;
	CouldHave.poison_swamp_access = CouldHave.cross_poisoned_water && (CouldHave.hookshot || CouldHave.bow || CouldHave.pictobox || CouldHave.zora_mask || CouldHave.any_bottle);
	CouldHave.limitless_magic_beans = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	CouldHave.any_magic_bean = (CouldHave.poison_swamp_access && CouldHave.deku_mask) || CouldHave.magic_bean; 
	
	CouldHave.wft_access = CouldHave.deku_mask && CouldHave.sonata && CouldHave.poison_swamp_access;
	CouldHave.woodfall_clear = CouldHave.wft_access && CouldHave.bow; 
	
	// Mountain area shortcuts
	CouldHave.north_access = CouldHave.bow && (CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.goron_mask);
	CouldHave.hot_spring_water = CouldHave.north_access && CouldHave.any_bottle && (CouldHave.goron_mask || CouldHave.shoot_fire_arrow || CouldHave.snowhead_clear || (CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.any_blue_potion));
	
	CouldHave.sht_access = CouldHave.north_access && CouldHave.goron_mask && CouldHave.lullaby;
	CouldHave.snowhead_clear = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	
	// Great Bay area shortcuts
	CouldHave.west_access = CouldHave.eponas_song;
	CouldHave.ocean_skulltulas = CouldHave.explosive && CouldHave.west_access && CouldHave.hookshot;
	CouldHave.pirates_fortress_access = CouldHave.west_access && CouldHave.zora_mask;
	CouldHave.pinnacle_rock_access = CouldHave.zora_mask;
	CouldHave.seahorse = CouldHave.pictobox && CouldHave.west_access && CouldHave.pirates_fortress_access;
	CouldHave.gbt_access = CouldHave.pirates_fortress_access && CouldHave.nwbn && CouldHave.hookshot;
	CouldHave.great_bay_clear = CouldHave.gbt_access && CouldHave.shoot_ice_arrow; 

	// Ikana area shortcuts
	CouldHave.ikana_graveyard_access = CouldHave.eponas_song;
	CouldHave.east_access = CouldHave.hookshot && CouldHave.ikana_graveyard_access && (CouldHave.garo_mask || CouldHave.gibdo_mask);
	CouldHave.ikana_canyon_access = CouldHave.east_access;
	CouldHave.stt_access = CouldHave.ikana_canyon_access && CouldHave.hookshot && CouldHave.elegy && (CouldHave.goron_mask || CouldHave.zora_mask);
	CouldHave.istt_access = CouldHave.stt_access && CouldHave.shoot_light_arrow;
	CouldHave.ikana_clear = CouldHave.istt_access;
	
	// Bottle shortcuts
	CouldHave.any_blue_potion = CouldHave.any_bottle && (CouldHave.adults_wallet || CouldHave.mask_of_scents);
	CouldHave.any_milk = CouldHave.any_bottle; // gorman bros purchase requires nothing but bottle
	CouldHave.zora_egg = CouldHave.any_bottle && CouldHave.pirates_fortress_access && CouldHave.hookshot && CouldHave.pinnacle_rock_access && ((CouldHave.deku_mask && CouldHave.magic) || CouldHave.bow);
	CouldHave.big_poe = CouldHave.any_bottle && CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.bomb && (CouldHave.shoot_light_arrow || (CouldHave.limitless_magic_beans && (CouldHave.bow || CouldHave.zora_mask)));
}

function update_location_logic() {
	
	// South Clock Town
	Location_Logic["Clock Tower Entrance"] = true;
	Location_Logic["South Clock Town Straw Roof Chest"] = Logic.hookshot || (Logic.deku_mask && Logic.moons_tear);
	Location_Logic["South Clock Town Final Day Chest"] = Logic.hookshot || (Logic.deku_mask && Logic.moons_tear);
	Location_Logic["Clock Town Scrub Trade"] = Logic.moons_tear;
	Location_Logic["Postbox"] = Logic.postmans_hat;
		
	// North Clock Town
	Location_Logic["Bombers' Hide and Seek"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
	Location_Logic["Clock Town Map Purchase"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
	Location_Logic["Deku Playground Any Day"] = Logic.deku_mask;
	Location_Logic["Keaton Quiz"] = Logic.keaton_mask;
	Location_Logic["North Clock Town Tree"] = true;
	Location_Logic["Old Lady"] = true;
	Location_Logic["Town Great Fairy"] = true;
	Location_Logic["Town Great Fairy Non-Human"] = Logic.deku_mask || Logic.goron_mask || Logic.zora_mask;
		
	// West Clock Town
	Location_Logic["Bomb Bag Purchase"] = true;
	Location_Logic["Big Bomb Bag Purchase"] = true;
	Location_Logic["Bank Reward #1"] = true;
	Location_Logic["Postman's Game"] = Logic.bunny_hood;
	Location_Logic["Rosa Sisters"] = Logic.kamaro_mask;
	Location_Logic["Swordsman's School"] = true;
	Location_Logic["All-Night Mask Purchase"] = Logic.giants_wallet;
	Location_Logic["Bank Reward #3"] = Logic.giants_wallet;
	Location_Logic["Bank Reward #2"] = Logic.adults_wallet;
		
	// Laundry Pool
	Location_Logic["Guru Guru"] = true;
	Location_Logic["Kafei"] = Logic.letter_to_kafei;
	Location_Logic["Curiosity Shop Man #1"] = Logic.letter_to_kafei;
	Location_Logic["Curiosity Shop Man #2"] = Logic.letter_to_kafei;
		
	// East Clock Town
	Location_Logic["Bombers' Hideout Chest"] = Logic.explosive && (Logic.deku_mask || Logic.zora_mask || Logic.bow || Logic.hookshot);
	Location_Logic["East Clock Town Chest"] = true;
	Location_Logic["Gorman"] = Logic.deku_mask && Logic.goron_mask && Logic.zora_mask && Logic.romani_mask;
	Location_Logic["Honey and Darling Any Day"] = Logic.bow || Logic.bomb || (Logic.deku_mask && Logic.magic);
	Location_Logic["Madame Aroma in Bar"] = Logic.special_delivery && Logic.kafei_mask;
	Location_Logic["Madame Aroma in Office"] = true;
	Location_Logic["Mayor"] = Logic.couples_mask;
	Location_Logic["Postman's Freedom Reward"] = Logic.special_delivery;
	Location_Logic["Town Archery #1"] = Logic.bow;
	Location_Logic["Treasure Chest Game Goron"] = Logic.goron_mask;
	Location_Logic["Milk Bar Milk"] = Logic.romani_mask;
	Location_Logic["Milk Bar Chateau"] = Logic.romani_mask && Logic.adults_wallet;
		
	// Stock Pot Inn
	Location_Logic["Inn Reservation"] = true;
	Location_Logic["Midnight Meeting"] = Logic.kafei_mask && Logic.night_inn_access;
	Location_Logic["Toilet Hand"] = Logic.land_title_deed || Logic.swamp_title_deed || Logic.mountain_title_deed || Logic.ocean_title_deed || Logic.letter_to_kafei || Logic.special_delivery;
	Location_Logic["Grandma Short Story"] = Logic.allnight_mask;
	Location_Logic["Grandma Long Story"] = Logic.allnight_mask;
	Location_Logic["Inn Staff Room Chest"] = true;
	Location_Logic["Inn Guest Room Chest"] = Logic.room_key;
		
	// Termina Field
	Location_Logic["Astronomy Telescope"] = Logic.deku_mask || Logic.hookshot || Logic.zora_mask || Logic.bow;
	Location_Logic["Gossip Stones"] = (Logic.goron_mask && Logic.lullaby) || (Logic.deku_mask && Logic.sonata && (Logic.explosive || Logic.goron_mask)) || (Logic.zora_mask && Logic.nwbn && (Logic.explosive || Logic.goron_mask));
	Location_Logic["Business Scrub Purchase"] = Logic.adults_wallet && (Logic.deku_mask || Logic.hookshot || Logic.zora_mask || Logic.bow);
	Location_Logic["Peahat Grotto"] = true;
	Location_Logic["Dodongo Grotto"] = true;
	Location_Logic["Kamaro"] = Logic.song_of_healing;
	Location_Logic["Termina Field Pillar Grotto"] = true;
	Location_Logic["Termina Field Grass Grotto"] = true;
	Location_Logic["Termina Field Underwater Chest"] = Logic.zora_mask;
	Location_Logic["Termina Field Grass Chest"] = true;
	Location_Logic["Termina Field Stump Chest"] = Logic.hookshot || (Logic.any_magic_bean && Logic.water_for_magic_bean); 
	Location_Logic["Bio Baba Grotto"] = Logic.zora_mask && (Logic.explosive || Logic.goron_mask);
		
	// Road to Southern Swamp
	Location_Logic["Swamp Archery #1"] = Logic.bow;
	Location_Logic["Swamp Archery #2"] = Logic.bow;
	Location_Logic["Path to Swamp Grotto"] = true;
	Location_Logic["Path to Swamp Tree"] = (Logic.deku_mask && Logic.magic) || Logic.bow || Logic.hookshot || Logic.zora_mask;
	Location_Logic["Woodfall Map Purchase"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
	Location_Logic["Snowhead Map Purchase"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
		
	// Southern Swamp
	Location_Logic["Koume"] = Logic.any_bottle;
	Location_Logic["Kotake"] = true;
	Location_Logic["Swamp Scrub Trade"] = Logic.land_title_deed;
	Location_Logic["Pictograph Contest Winner"] = Logic.pictobox;
	Location_Logic["Boat Archery"] = Logic.woodfall_clear && Logic.any_bottle; 
	
	Location_Logic["Swamp Spider House Reward"] = Logic.poison_swamp_access && (Logic.hookshot || Logic.zora_mask || (Logic.deku_mask && (Logic.bow || Logic.magic || Logic.bomb))) && (Logic.hookshot || Logic.zora_mask || (Logic.deku_mask && Logic.water_for_magic_bean)) && Logic.any_bottle && (Logic.hookshot || Logic.zora_mask || (Logic.water_for_magic_bean && Logic.any_magic_bean && (Logic.goron_mask || Logic.explosive))) && (Logic.hookshot || Logic.zora_mask || Logic.bow || (Logic.deku_mask && Logic.magic));
	// can get without hookshot or zora mask with beans: https://www.twitch.tv/videos/1032053901
	
	Location_Logic["Near Swamp Spider House Grotto"] = Logic.poison_swamp_access && (Logic.deku_mask || Logic.zora_mask);
	Location_Logic["Mystery Woods Grotto"] = true;
	Location_Logic["Swamp Tourist Center Roof"] = Logic.deku_mask && Logic.land_title_deed;
		
	// Deku Palace
	Location_Logic["Bean Man"] = Logic.poison_swamp_access && Logic.deku_mask;
	Location_Logic["Butler"] = Logic.woodfall_clear && Logic.poison_swamp_access && Logic.deku_mask && Logic.any_bottle; 
	Location_Logic["Bean Grotto"] = Logic.poison_swamp_access && (((Logic.hookshot || Logic.water_for_magic_bean) && Logic.deku_mask)); 
	Location_Logic["Deku Palace West Garden"] = Logic.poison_swamp_access && Logic.deku_mask;
		
	// Woodfall
	Location_Logic["Woodfall Bridge Chest"] = Logic.poison_swamp_access && Logic.deku_mask;
	Location_Logic["Behind Woodfall Owl Chest"] = Logic.poison_swamp_access && Logic.deku_mask;
	Location_Logic["Entrance to Woodfall Chest"] = Logic.poison_swamp_access && Logic.deku_mask;
		
	// Milk Road
	Location_Logic["Gorman Bros Race"] = Logic.eponas_song;
	Location_Logic["Romani Ranch Map Purchase"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
	Location_Logic["Great Bay Map Purchase"] = (Logic.deku_mask && Logic.magic) || Logic.zora_mask || Logic.bow || Logic.hookshot;
	Location_Logic["Gorman Bros Milk Purchase"] = true;
	
	// Romani Ranch
	Location_Logic["Aliens Defense"] = Logic.ranch_day_1_access && Logic.bow;
	Location_Logic["Dog Race"] = Logic.mask_of_truth;
	Location_Logic["Grog"] = Logic.bremen_mask;
	Location_Logic["Cremia"] = Logic.ranch_day_1_access && Logic.bow;
	Location_Logic["Doggy Racetrack Roof Chest"] = Logic.zora_mask || Logic.hookshot || (Logic.any_magic_bean && Logic.water_for_magic_bean);
		
	// Mountain Village
	Location_Logic["Mountain Smithy Day 1"] = Logic.north_access && Logic.adults_wallet && (Logic.shoot_fire_arrow || Logic.hot_spring_water || Logic.snowhead_clear);
	Location_Logic["Mountain Smithy Day 2"] = Logic.north_access && Logic.bottle_gold_dust && (Logic.shoot_fire_arrow || Logic.hot_spring_water || Logic.snowhead_clear);
	Location_Logic["Frog Choir"] = Logic.dongero_mask && Logic.snowhead_clear && Logic.wft_access && Logic.gbt_access && Logic.hookshot && Logic.deku_mask && Logic.zora_mask && Logic.shoot_ice_arrow && Logic.shoot_fire_arrow;
	Location_Logic["Hungry Goron"] = Logic.north_access && Logic.goron_mask && Logic.magic;
	Location_Logic["Darmani"] = Logic.song_of_healing && Logic.north_access && Logic.lens && Logic.magic;
	Location_Logic["Mountain Waterfall Chest"] = Logic.snowhead_clear;
	Location_Logic["Mountain Spring Grotto"] = Logic.snowhead_clear;
		
	// Twin Islands
	Location_Logic["Goron Race"] = Logic.snowhead_clear && Logic.goron_mask && Logic.magic;
	Location_Logic["Twin Islands Underwater Ramp Chest"] = Logic.snowhead_clear && Logic.zora_mask;
	Location_Logic["Hot Spring Water Grotto"] = Logic.north_access && Logic.explosive && (Logic.shoot_fire_arrow || Logic.hot_spring_water || Logic.snowhead_clear);
	Location_Logic["Twin Islands Cave Chest"] = Logic.snowhead_clear && Logic.zora_mask;
	Location_Logic["Goron Racetrack Grotto"] = Logic.north_access && Logic.explosive && (Logic.goron_mask || Logic.hookshot); // hookshot also needs scarecrow song
	
	// Goron Village
	Location_Logic["Goron Shop 10 Arrows"] = Logic.north_access;
	Location_Logic["Goron Shop 10 Bombs"] = Logic.north_access;
	Location_Logic["Goron Shop Red Potion"] = Logic.north_access;
	Location_Logic["Powder Keg Challenge"] = Logic.north_access && Logic.goron_mask && (Logic.shoot_fire_arrow || Logic.snowhead_clear);
	Location_Logic["Lens of Truth Chest"] = Logic.north_access;
	Location_Logic["Biggest Bomb Bag Purchase"] = Logic.north_access && Logic.adults_wallet && (
		Logic.goron_mask // buy from mountain village
		|| (Logic.deku_mask && Logic.moons_tear && Logic.land_title_deed && Logic.swamp_title_deed) // move the scrub from clock town, then from swamp to clock town, and from mountain to swamp, then buy from swamp one as any form 
	);
	Location_Logic["Mountain Scrub Trade"] = Logic.north_access && Logic.deku_mask && Logic.swamp_title_deed;
	Location_Logic["Lens Cave Invisible Chest"] = Logic.north_access;
	Location_Logic["Lens Cave Rock Chest"] = Logic.north_access && Logic.explosive;
	Location_Logic["Goron Village Ledge"] = Logic.north_access && Logic.deku_mask && Logic.swamp_title_deed;
		
	// Path to Snowhead
	Location_Logic["Path to Snowhead Grotto"] = Logic.north_access && Logic.explosive && Logic.goron_mask && Logic.magic;
	Location_Logic["Path to Snowhead Pillar"] = Logic.north_access && Logic.hookshot && Logic.goron_mask && Logic.magic && Logic.lens;
		
	// Great Bay Coast
	Location_Logic["Ocean Spider House Day 1 Reward"] = Logic.ocean_skulltulas && Logic.shoot_fire_arrow;
	Location_Logic["Fisherman Game"] = Logic.great_bay_clear && Logic.hookshot;
	Location_Logic["Ocean Spider House Chest"] = Logic.bow && Logic.ocean_skulltulas && Logic.captains_hat;
	Location_Logic["Mikau"] = Logic.west_access && Logic.song_of_healing;
	Location_Logic["Great Bay Coast Grotto"] = Logic.west_access;
	Location_Logic["Lab Fish"] = Logic.west_access && Logic.any_bottle;
	Location_Logic["Great Bay Coast Ledge"] = Logic.west_access && Logic.hookshot && Logic.any_magic_bean && Logic.water_for_magic_bean;
	Location_Logic["Stone Tower Map Purchase"] = Logic.west_access && (Logic.hookshot || Logic.bow);
	Location_Logic["Fisherman Pictograph"] = Logic.pictobox && Logic.west_access && Logic.pirates_fortress_access;
		
	// Zora Cape
	Location_Logic["Beaver Race #1"] = Logic.hookshot && Logic.zora_mask && Logic.west_access;
	Location_Logic["Beaver Race #2"] = Logic.hookshot && Logic.zora_mask && Logic.west_access;
	Location_Logic["Zora Cape Ledge Without Tree Chest"] = Logic.hookshot && Logic.west_access;
	Location_Logic["Zora Cape Ledge With Tree Chest"] = Logic.hookshot && Logic.west_access;
	Location_Logic["Zora Cape Grotto"] = Logic.west_access && (Logic.goron_mask || Logic.explosive);
	Location_Logic["Zora Cape Underwater Chest"] = Logic.zora_mask && Logic.west_access;
	Location_Logic["Zora Cape Like-Like"] = Logic.pirates_fortress_access;
		
	// Zora Hall
	Location_Logic["Zora Shop 10 Arrows"] = Logic.zora_mask && Logic.west_access;
	Location_Logic["Zora Shop Hero's Shield"] = Logic.zora_mask && Logic.west_access;
	Location_Logic["Zora Shop Red Potion"] = Logic.zora_mask && Logic.west_access;
	Location_Logic["Ocean Scrub Trade"] = Logic.goron_mask && Logic.mountain_title_deed && Logic.zora_mask && Logic.west_access;
	Location_Logic["Evan"] = Logic.zora_mask && Logic.west_access;
	Location_Logic["Lulu's Room Ledge"] = Logic.pirates_fortress_access && Logic.goron_mask && Logic.deku_mask && Logic.mountain_title_deed;
	Location_Logic["Zora Hall Stage Lights"] = Logic.zora_mask && Logic.west_access && Logic.shoot_fire_arrow;
		
	// Pirates' Fortress Exterior
	Location_Logic["Pirates' Fortress Exterior Log Chest"] = Logic.pirates_fortress_access;
	Location_Logic["Pirates' Fortress Exterior Sand Chest"] = Logic.pirates_fortress_access;
	Location_Logic["Pirates' Fortress Exterior Corner Chest"] = Logic.pirates_fortress_access;
		
	// Pirates' Fortress Sewer
	Location_Logic["Pirates' Fortress Cage Room Shallow Chest"] = Logic.pirates_fortress_access && Logic.goron_mask;
	Location_Logic["Pirates' Fortress Cage Room Deep Chest"] = Logic.pirates_fortress_access && Logic.goron_mask;
	Location_Logic["Pirates' Fortress Maze Chest"] = Logic.pirates_fortress_access && Logic.goron_mask;
	Location_Logic["Pirates' Fortress Cage"] = Logic.pirates_fortress_access && Logic.goron_mask;
		
	// Pirates' Fortress Interior
	Location_Logic["Hookshot Chest"] = Logic.pirates_fortress_access && ((Logic.bow || (Logic.deku_mask && Logic.magic)) && (Logic.goron_mask || Logic.hookshot));
	Location_Logic["Pirates' Fortress Interior Lower Chest"] = Logic.pirates_fortress_access && (Logic.goron_mask || Logic.hookshot);
	Location_Logic["Pirates' Fortress Interior Upper Chest"] = Logic.pirates_fortress_access && Logic.hookshot;
	Location_Logic["Pirates' Fortress Interior Tank Chest"] = Logic.pirates_fortress_access && Logic.hookshot;
	Location_Logic["Pirates' Fortress Interior Guard Room Chest"] = Logic.pirates_fortress_access && Logic.hookshot;
		
	// Pinnacle Rock
	Location_Logic["Seahorses"] = Logic.west_access && Logic.zora_mask && Logic.any_bottle && Logic.seahorse;
	Location_Logic["Pinnacle Rock Lower Chest"] = Logic.pinnacle_rock_access && Logic.west_access;
	Location_Logic["Pinnacle Rock Upper Chest"] = Logic.pinnacle_rock_access && Logic.west_access;
		
	// Path to Ikana Canyon
	Location_Logic["Invisible Soldier"] = Logic.lens && Logic.magic && Logic.any_bottle && Logic.eponas_song;
	Location_Logic["Path to Ikana Pillar Chest"] = Logic.hookshot;
	Location_Logic["Path to Ikana Grotto"] = Logic.goron_mask;
		
	// Ikana Graveyard
	Location_Logic["Dampe Digging"] = Logic.captains_hat && Logic.ikana_graveyard_access && (Logic.zora_mask || Logic.bow);
	Location_Logic["Iron Knuckle Chest"] = Logic.captains_hat && Logic.ikana_graveyard_access && Logic.explosive;
	Location_Logic["Captain Keeta's Chest"] = Logic.sonata && Logic.ikana_graveyard_access && Logic.bow;
	Location_Logic["Day 1 Grave Bats"] = Logic.captains_hat && Logic.ikana_graveyard_access;
	Location_Logic["Ikana Graveyard Grotto"] = Logic.ikana_graveyard_access && Logic.explosive;
		
	// Ikana Canyon
	Location_Logic["Poe Hut"] = Logic.ikana_canyon_access && Logic.bow;
	Location_Logic["Pamela's Father"] = Logic.song_of_healing && Logic.ikana_canyon_access && Logic.song_of_storms;
	Location_Logic["Secret Shrine Grotto"] = Logic.east_access;
	Location_Logic["Ikana Canyon Ledge"] = Logic.zora_mask && Logic.ocean_title_deed && Logic.deku_mask && Logic.east_access;
	Location_Logic["Canyon Scrub Trade"] = Logic.zora_mask && Logic.ocean_title_deed && Logic.east_access;
		
	// Secret Shrine
	Location_Logic["Secret Shrine Final Chest"] = Logic.shoot_light_arrow && Logic.east_access;
	Location_Logic["Secret Shrine Dinolfos Chest"] = Logic.shoot_light_arrow && Logic.east_access;
	Location_Logic["Secret Shrine Wizzrobe Chest"] = Logic.shoot_light_arrow && Logic.east_access;
	Location_Logic["Secret Shrine Wart Chest"] = Logic.shoot_light_arrow && Logic.east_access;
	Location_Logic["Secret Shrine Garo Master Chest"] = Logic.shoot_light_arrow && Logic.east_access;
		
	// Beneath the Well 
	Location_Logic["Mirror Shield Chest"] = Logic.ikana_canyon_access && ((Logic.shoot_fire_arrow && Logic.shoot_light_arrow) || (Logic.shoot_light_arrow && Logic.gibdo_mask && Logic.any_milk && Logic.big_poe) || (Logic.gibdo_mask && Logic.big_poe && Logic.any_milk && Logic.limitless_magic_beans));
	Location_Logic["Well Right Path Chest"] = Logic.ikana_canyon_access && Logic.gibdo_mask && Logic.any_bottle && (Logic.shoot_light_arrow || Logic.limitless_magic_beans);
	Location_Logic["Well Left Path Chest"] = Logic.ikana_canyon_access && Logic.gibdo_mask && Logic.any_blue_potion;
		
	// Ikana Castle
	Location_Logic["Ikana Castle Pillar"] = Logic.ikana_canyon_access && Logic.deku_mask && Logic.shoot_fire_arrow && Logic.lens && (Logic.shoot_light_arrow || Logic.mirror_shield);
		
	// Stone Tower
	Location_Logic["Inverted Stone Tower Left Chest"] = Logic.istt_access && Logic.any_magic_bean && Logic.water_for_magic_bean;
	Location_Logic["Inverted Stone Tower Middle Chest"] = Logic.istt_access && Logic.any_magic_bean && Logic.water_for_magic_bean;
	Location_Logic["Inverted Stone Tower Right Chest"] = Logic.istt_access && Logic.any_magic_bean && Logic.water_for_magic_bean;
	
	// Woodfall Temple
	Location_Logic["Woodfall Entrance Platform"] = Logic.wft_access;
	Location_Logic["Woodfall Dark Room"] = Logic.wft_access;
	Location_Logic["Woodfall Main Room Switch"] = Logic.wft_access;
	Location_Logic["Hero's Bow Chest"] = Logic.wft_access;
	Location_Logic["Woodfall Map Chest"] = Logic.wft_access;
	Location_Logic["Woodfall Small Key Chest"] = Logic.wft_access;
	Location_Logic["Woodfall Compass Chest"] = Logic.wft_access;
	Location_Logic["Woodfall Boss Key Chest"] = Logic.wft_access && Logic.bow;
	Location_Logic["Odolwa Heart Container"] = Logic.wft_access && Logic.bow;
	
	// Snowhead Temple
	Location_Logic["Fire Arrow Chest"] = Logic.sht_access && (Logic.hookshot || Logic.explosive || Logic.shoot_fire_arrow);
	Location_Logic["Snowhead Boss Key Chest"] = Logic.sht_access && Logic.shoot_fire_arrow;
	Location_Logic["Goht Heart Container"] = Logic.sht_access && Logic.shoot_fire_arrow;
	Location_Logic["Snowhead Basement"] = Logic.sht_access;
	Location_Logic["Snowhead Block Room Chest"] = Logic.sht_access;
	Location_Logic["Snowhead Bridge Room Chest"] = Logic.sht_access && (Logic.hookshot || Logic.shoot_fire_arrow);
	Location_Logic["Snowhead Compass Chest"] = Logic.sht_access;
	Location_Logic["Snowhead Ice Puzzle"] = Logic.sht_access && Logic.shoot_fire_arrow; 
	Location_Logic["Snowhead Icicle Room Chest"] = Logic.sht_access && (Logic.hookshot || Logic.explosive || Logic.shoot_fire_arrow);
	Location_Logic["Snowhead Icicle Room Wall"] = Logic.sht_access && (Logic.hookshot || Logic.explosive || Logic.shoot_fire_arrow);
	Location_Logic["Snowhead Main Room Wall"] = Logic.sht_access && (Logic.hookshot || Logic.shoot_fire_arrow);
	Location_Logic["Snowhead Map Chest"] = Logic.sht_access;
	Location_Logic["Snowhead Map Room Ledge"] = Logic.sht_access && (Logic.hookshot || (Logic.lens && (Logic.explosive || Logic.shoot_fire_arrow)));
	Location_Logic["Snowhead Pillar Freezards"] = Logic.sht_access && Logic.shoot_fire_arrow;
	Location_Logic["Snowhead Twin Block"] = Logic.sht_access && (Logic.hookshot || Logic.shoot_fire_arrow || Logic.zora_mask);
	
	// Great Bay Temple
	Location_Logic["Great Bay Entrance Torches"] = Logic.gbt_access;
	Location_Logic["Great Bay Bio Babas"] = Logic.gbt_access;
	Location_Logic["Great Bay Green Valve"] = Logic.gbt_access && Logic.shoot_ice_arrow;
	Location_Logic["Great Bay Seesaw Room"] = Logic.gbt_access && Logic.shoot_ice_arrow && Logic.shoot_fire_arrow;
	Location_Logic["Great Bay Waterwheel Room Lower"] = Logic.gbt_access && Logic.shoot_ice_arrow;
	Location_Logic["Great Bay Waterwheel Room Upper"] = Logic.gbt_access && Logic.shoot_ice_arrow;
	Location_Logic["Ice Arrow Chest"] = Logic.gbt_access;
	Location_Logic["Great Bay Boss Key Chest"] = Logic.gbt_access && Logic.shoot_ice_arrow && Logic.shoot_fire_arrow;
	Location_Logic["Great Bay Map Chest"] = Logic.gbt_access;
	Location_Logic["Great Bay Compass Chest"] = Logic.gbt_access;
	Location_Logic["Great Bay Small Key Chest"] = Logic.gbt_access;
	Location_Logic["Gyorg Heart Container"] = Logic.gbt_access && Logic.shoot_ice_arrow && Logic.shoot_fire_arrow;
	
	// Stone Tower Temple
	Location_Logic["Stone Tower Statue Eye"] = Logic.stt_access && Logic.bow;
	Location_Logic["Stone Tower Compass Chest"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive));
	Location_Logic["Stone Tower Underwater"] = Logic.istt_access && Logic.zora_mask;
	Location_Logic["Stone Tower Eyegore Room Chest"] = Logic.stt_access && ((Logic.shoot_light_arrow && Logic.zora_mask) || (Logic.explosive && Logic.goron_mask));
	Location_Logic["Stone Tower Bridge Crystal"] = Logic.stt_access && Logic.shoot_light_arrow && Logic.zora_mask;
	Location_Logic["Stone Tower Basement Ledge"] = Logic.stt_access && ((Logic.explosive && Logic.goron_mask) || (Logic.shoot_light_arrow && Logic.zora_mask));
	Location_Logic["Stone Tower Map Chest"] = Logic.stt_access && (((Logic.mirror_shield || Logic.shoot_light_arrow) && Logic.explosive && Logic.goron_mask) || (Logic.shoot_light_arrow && Logic.zora_mask));
	Location_Logic["Stone Tower Armos Room Chest"] = Logic.stt_access && (((Logic.mirror_shield || Logic.shoot_light_arrow) && Logic.explosive && Logic.goron_mask) || (Logic.shoot_light_arrow && Logic.zora_mask));
	Location_Logic["Stone Tower Mirror Sun Switch"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive));
	Location_Logic["Stone Tower Mirror Sun Block"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive));
	Location_Logic["Stone Tower Lava Room Fire Ring"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive)) && Logic.goron_mask && (Logic.shoot_light_arrow || Logic.deku_mask);
	Location_Logic["Stone Tower Lava Room Ledge"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive)) && Logic.deku_mask;
	Location_Logic["Light Arrow Chest"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.deku_mask && Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive));
	Location_Logic["Stone Tower Thin Bridge"] = (Logic.stt_access && (Logic.shoot_light_arrow || (Logic.deku_mask && Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive)) && Logic.explosive) || (Logic.istt_access && Logic.deku_mask);
	Location_Logic["Stone Tower Eyegore"] = Logic.stt_access && (Logic.shoot_light_arrow || (Logic.deku_mask && Logic.mirror_shield && Logic.goron_mask && Logic.zora_mask && Logic.explosive));
	Location_Logic["Stone Tower Death Armos"] = Logic.istt_access && Logic.deku_mask;
	
	// Inverted Stone Tower Temple
	Location_Logic["Stone Tower Entrance Sun Switch"] = Logic.istt_access;
	Location_Logic["Stone Tower Updraft Frozen Eye"] = Logic.istt_access && Logic.deku_mask && Logic.shoot_fire_arrow;
	Location_Logic["Stone Tower Updraft Fire Ring"] = Logic.istt_access && Logic.zora_mask && Logic.deku_mask;
	Location_Logic["Stone Tower Updraft Room Chest"] = Logic.istt_access && Logic.deku_mask;
	Location_Logic["Giant's Mask Chest"] = Logic.istt_access && Logic.deku_mask;
	Location_Logic["Stone Tower Death Armos Maze Chest"] = Logic.istt_access && Logic.deku_mask;
	Location_Logic["Stone Tower Wizzrobe"] = Logic.istt_access && Logic.deku_mask;
	Location_Logic["Stone Tower Boss Key Chest"] = Logic.istt_access && Logic.deku_mask;
	Location_Logic["Twinmold Heart Container"] = Logic.istt_access && Logic.deku_mask && (Logic.giants_mask || Logic.fiercedeity_mask);
		
	// Songs
	Location_Logic["Starting Song"] = true;
	Location_Logic["Boss Blue Warp"] = Logic.woodfall_clear || Logic.snowhead_clear || Logic.great_bay_clear || Logic.ikana_clear;
	Location_Logic["Romani's Game"] = Logic.ranch_day_1_access;
	Location_Logic["Day 1 Grave Tablet"] = Logic.captains_hat && Logic.ikana_graveyard_access;
	Location_Logic["Imprisoned Monkey"] = Logic.poison_swamp_access && Logic.deku_mask;
	Location_Logic["Swamp Music Statue"] = Logic.poison_swamp_access && Logic.deku_mask;
	Location_Logic["Baby Goron"] = Logic.north_access && Logic.goron_mask;
	Location_Logic["Baby Zoras"] = Logic.zora_egg && Logic.zora_mask && Logic.west_access;
	Location_Logic["Ikana King"] = Logic.ikana_canyon_access && Logic.shoot_fire_arrow && Logic.mirror_shield && (Logic.shoot_light_arrow || (Logic.deku_mask && Logic.powder_keg && Logic.goron_mask && Logic.lens));
	Location_Logic["Skull Kid Song"] = (Logic.deku_mask && Logic.magic) || Logic.bow || Logic.zora_mask || Logic.hookshot;
	
	// South Clock Town
	Location_Access["Clock Tower Entrance"] = true;
	Location_Access["South Clock Town Straw Roof Chest"] = true;
	Location_Access["South Clock Town Final Day Chest"] = Game.hookshot || (Game.deku_mask && Game.moons_tear);
	Location_Access["Clock Town Scrub Trade"] = Game.moons_tear;
	Location_Access["Postbox"] = Game.postmans_hat;
		
	// North Clock Town
	Location_Access["Bombers' Hide and Seek"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
	Location_Access["Clock Town Map Purchase"] = true;
	Location_Access["Deku Playground Any Day"] = Game.deku_mask;
	Location_Access["Keaton Quiz"] = Game.keaton_mask;
	Location_Access["North Clock Town Tree"] = true;
	Location_Access["Old Lady"] = true;
	Location_Access["Town Great Fairy"] = true;
	Location_Access["Town Great Fairy Non-Human"] = Game.deku_mask || Game.goron_mask || Game.zora_mask;
		
	// West Clock Town
	Location_Access["Bomb Bag Purchase"] = true;
	Location_Access["Big Bomb Bag Purchase"] = true;
	Location_Access["Bank Reward #1"] = true;
	Location_Access["Postman's Game"] = true;
	Location_Access["Rosa Sisters"] = Game.kamaro_mask;
	Location_Access["Swordsman's School"] = true;
	Location_Access["All-Night Mask Purchase"] = true;
	Location_Access["Bank Reward #3"] = true;
	Location_Access["Bank Reward #2"] = true;
		
	// Laundry Pool
	Location_Access["Guru Guru"] = true;
	Location_Access["Kafei"] = Game.letter_to_kafei;
	Location_Access["Curiosity Shop Man #1"] = Game.letter_to_kafei;
	Location_Access["Curiosity Shop Man #2"] = Game.letter_to_kafei;
		
	// East Clock Town
	Location_Access["Bombers' Hideout Chest"] = Game.explosive;
	Location_Access["East Clock Town Chest"] = true;
	Location_Access["Gorman"] = Game.deku_mask && Game.goron_mask && Game.zora_mask && Game.romani_mask;
	Location_Access["Honey and Darling Any Day"] = Game.bow || Game.bomb || (Game.deku_mask && Game.magic);
	Location_Access["Madame Aroma in Bar"] = Game.special_delivery && Game.kafei_mask;
	Location_Access["Madame Aroma in Office"] = true;
	Location_Access["Mayor"] = Game.couples_mask;
	Location_Access["Postman's Freedom Reward"] = Game.special_delivery;
	Location_Access["Town Archery #1"] = Game.bow;
	Location_Access["Treasure Chest Game Goron"] = Game.goron_mask;
	Location_Access["Milk Bar Milk"] = Game.romani_mask;
	Location_Access["Milk Bar Chateau"] = Game.romani_mask;
		
	// Stock Pot Inn
	Location_Access["Inn Reservation"] = true;
	Location_Access["Midnight Meeting"] = Game.kafei_mask/* && Game.night_inn_access*/; // does require night inn access, but removed that to remind to set up meeting...
	Location_Access["Toilet Hand"] = Game.land_title_deed || Game.swamp_title_deed || Game.mountain_title_deed || Game.ocean_title_deed || Game.letter_to_kafei || Game.special_delivery; // can go in inn night 3 without access
	Location_Access["Grandma Short Story"] = Game.allnight_mask;
	Location_Access["Grandma Long Story"] = Game.allnight_mask;
	Location_Access["Inn Staff Room Chest"] = true;
	Location_Access["Inn Guest Room Chest"] = Game.room_key;
		
	// Termina Field
	Location_Access["Astronomy Telescope"] = true;
	Location_Access["Gossip Stones"] = (Game.goron_mask && Game.lullaby) || (Game.deku_mask && Game.sonata && (Game.explosive || Game.goron_mask)) || (Game.zora_mask && Game.nwbn && (Game.explosive || Game.goron_mask));
	Location_Access["Business Scrub Purchase"] = true;
	Location_Access["Peahat Grotto"] = true;
	Location_Access["Dodongo Grotto"] = true;
	Location_Access["Kamaro"] = Game.song_of_healing;
	Location_Access["Termina Field Pillar Grotto"] = true;
	Location_Access["Termina Field Grass Grotto"] = true;
	Location_Access["Termina Field Underwater Chest"] = Game.zora_mask;
	Location_Access["Termina Field Grass Chest"] = true;
	Location_Access["Termina Field Stump Chest"] = Game.hookshot || (Game.any_magic_bean && Game.water_for_magic_bean) || (Game.bomb && Game.goron_mask); 
	Location_Access["Bio Baba Grotto"] = (Game.zora_mask || Game.hookshot || Game.bow || (Game.deku_mask && Game.magic)) && (Game.explosive || Game.goron_mask);
		
	// Road to Southern Swamp
	Location_Access["Swamp Archery #1"] = Game.bow;
	Location_Access["Swamp Archery #2"] = Game.bow;
	Location_Access["Path to Swamp Grotto"] = true;
	Location_Access["Path to Swamp Tree"] = true;
	Location_Access["Woodfall Map Purchase"] = true;
	Location_Access["Snowhead Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
		
	// Southern Swamp
	Location_Access["Koume"] = Game.any_bottle;
	Location_Access["Kotake"] = true;
	Location_Access["Swamp Scrub Trade"] = Game.land_title_deed;
	Location_Access["Pictograph Contest Winner"] = Game.pictobox;
	Location_Access["Boat Archery"] = Game.woodfall_clear && Game.any_bottle; 
	
	Location_Access["Swamp Spider House Reward"] = Game.poison_swamp_access && (Game.hookshot || Game.zora_mask || (Game.deku_mask && (Game.bow || Game.magic || Game.bomb))) && (Game.hookshot || Game.zora_mask || (Game.deku_mask && Game.water_for_magic_bean)) && Game.any_bottle && (Game.hookshot || Game.zora_mask || (Game.water_for_magic_bean && Game.any_magic_bean && (Game.goron_mask || Game.explosive))) && (Game.hookshot || Game.zora_mask || Game.bow || (Game.deku_mask && Game.magic));
	
	Location_Access["Near Swamp Spider House Grotto"] = Game.poison_swamp_access && (Game.deku_mask || Game.zora_mask);
	Location_Access["Mystery Woods Grotto"] = true;
	Location_Access["Swamp Tourist Center Roof"] = true;
		
	// Deku Palace
	Location_Access["Bean Man"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Access["Butler"] = Game.woodfall_clear && Game.poison_swamp_access && Game.deku_mask && Game.any_bottle; 
	Location_Access["Bean Grotto"] = Game.poison_swamp_access && (Game.hookshot || Game.water_for_magic_bean) && Game.deku_mask; 
	Location_Access["Deku Palace West Garden"] = Game.poison_swamp_access && Game.deku_mask;
	
	// Woodfall
	Location_Access["Woodfall Bridge Chest"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Access["Behind Woodfall Owl Chest"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Access["Entrance to Woodfall Chest"] = Game.poison_swamp_access && Game.deku_mask;
		
	// Milk Road
	Location_Access["Gorman Bros Milk Purchase"] = true;
	Location_Access["Gorman Bros Race"] = Game.eponas_song;
	Location_Access["Romani Ranch Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
	Location_Access["Great Bay Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
		
	// Romani Ranch
	Location_Access["Aliens Defense"] = Game.ranch_day_1_access && Game.bow;
	Location_Access["Dog Race"] = true;
	Location_Access["Grog"] = Game.bremen_mask;
	Location_Access["Cremia"] = Game.ranch_day_1_access && Game.bow;
	Location_Access["Doggy Racetrack Roof Chest"] = true;
		
	// Mountain Village
	Location_Access["Mountain Smithy Day 1"] = Game.north_access && Game.adults_wallet && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Access["Mountain Smithy Day 2"] = Game.north_access && Game.bottle_gold_dust && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Access["Frog Choir"] = Game.dongero_mask && Game.snowhead_clear && Game.wft_access && Game.gbt_access && Game.hookshot && Game.deku_mask && Game.zora_mask && Game.shoot_ice_arrow && Game.shoot_fire_arrow;
	Location_Access["Hungry Goron"] = Game.north_access && Game.goron_mask && Game.magic;
	Location_Access["Darmani"] = Game.song_of_healing && Game.north_access && Game.lens && Game.magic;
	Location_Access["Mountain Waterfall Chest"] = Game.snowhead_clear;
	Location_Access["Mountain Spring Grotto"] = Game.snowhead_clear;
		
	// Twin Islands
	Location_Access["Goron Race"] = Game.snowhead_clear && Game.goron_mask && Game.magic;
	Location_Access["Twin Islands Underwater Ramp Chest"] = Game.snowhead_clear && Game.zora_mask;
	Location_Access["Hot Spring Water Grotto"] = Game.north_access && Game.explosive && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Access["Twin Islands Cave Chest"] = Game.snowhead_clear && Game.zora_mask;
	Location_Access["Goron Racetrack Grotto"] = Game.north_access && Game.explosive; // hookshot also needs scarecrow song
		
	// Goron Village
	Location_Access["Goron Shop 10 Arrows"] = Game.north_access;
	Location_Access["Goron Shop 10 Bombs"] = Game.north_access;
	Location_Access["Goron Shop Red Potion"] = Game.north_access;
	Location_Access["Powder Keg Challenge"] = Game.north_access && Game.goron_mask && (Game.shoot_fire_arrow || Game.snowhead_clear);
	Location_Access["Lens of Truth Chest"] = Game.north_access;
	Location_Access["Biggest Bomb Bag Purchase"] = Game.north_access && (Game.goron_mask || Game.deku_mask);
	Location_Access["Mountain Scrub Trade"] = Game.north_access && Game.deku_mask && Game.swamp_title_deed;
	Location_Access["Lens Cave Invisible Chest"] = Game.north_access;
	Location_Access["Lens Cave Rock Chest"] = Game.north_access && Game.explosive;
	Location_Access["Goron Village Ledge"] = Game.north_access;
		
	// Path to Snowhead
	Location_Access["Path to Snowhead Grotto"] = Game.north_access && Game.explosive && Game.goron_mask;
	Location_Access["Path to Snowhead Pillar"] = Game.north_access && Game.goron_mask;
		
	// Great Bay Coast
	Location_Access["Ocean Spider House Day 1 Reward"] = Game.ocean_skulltulas;
	Location_Access["Fisherman Game"] = Game.great_bay_clear && Game.hookshot;
	Location_Access["Ocean Spider House Chest"] = Game.bow && Game.ocean_skulltulas;
	Location_Access["Mikau"] = Game.west_access && Game.song_of_healing;
	Location_Access["Great Bay Coast Grotto"] = Game.west_access;
	Location_Access["Lab Fish"] = Game.west_access && Game.any_bottle;
	Location_Access["Great Bay Coast Ledge"] = Game.west_access; // roll and c-up to peek. need either (bean and water) or (bomb)
	Location_Access["Stone Tower Map Purchase"] = Game.west_access && (Game.hookshot || Game.bow);
	Location_Access["Fisherman Pictograph"] = Game.pictobox && Game.west_access && Game.pirates_fortress_access;
		
	// Zora Cape
	Location_Access["Beaver Race #1"] = Game.hookshot && Game.zora_mask && Game.west_access;
	Location_Access["Beaver Race #2"] = Game.hookshot && Game.zora_mask && Game.west_access;
	Location_Access["Zora Cape Ledge Without Tree Chest"] = Game.hookshot && Game.west_access;
	Location_Access["Zora Cape Ledge With Tree Chest"] = Game.hookshot && Game.west_access;
	Location_Access["Zora Cape Grotto"] = Game.west_access && (Game.goron_mask || Game.explosive);
	Location_Access["Zora Cape Underwater Chest"] = Game.zora_mask && Game.west_access;
	Location_Access["Zora Cape Like-Like"] = Game.west_access && (Game.zora_mask || Game.bow);
		
	// Zora Hall
	Location_Access["Zora Shop 10 Arrows"] = Game.zora_mask && Game.west_access;
	Location_Access["Zora Shop Hero's Shield"] = Game.zora_mask && Game.west_access;
	Location_Access["Zora Shop Red Potion"] = Game.zora_mask && Game.west_access;
	Location_Access["Ocean Scrub Trade"] = Game.goron_mask && Game.mountain_title_deed && Game.zora_mask && Game.west_access;
	Location_Access["Evan"] = Game.zora_mask && Game.west_access;
	Location_Access["Lulu's Room Ledge"] = Game.pirates_fortress_access;
	Location_Access["Zora Hall Stage Lights"] = Game.shoot_fire_arrow && Game.west_access;
		
	// Pirates' Fortress Exterior
	Location_Access["Pirates' Fortress Exterior Log Chest"] = Game.pirates_fortress_access;
	Location_Access["Pirates' Fortress Exterior Sand Chest"] = Game.pirates_fortress_access;
	Location_Access["Pirates' Fortress Exterior Corner Chest"] = Game.pirates_fortress_access;
		
	// Pirates' Fortress Sewer
	Location_Access["Pirates' Fortress Cage Room Shallow Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Access["Pirates' Fortress Cage Room Deep Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Access["Pirates' Fortress Maze Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Access["Pirates' Fortress Cage"] = Game.pirates_fortress_access && Game.goron_mask;
		
	// Pirates' Fortress Interior
	Location_Access["Hookshot Chest"] = Game.pirates_fortress_access && ((Game.bow || (Game.deku_mask && Game.magic)) && (Game.goron_mask || Game.hookshot));
	Location_Access["Pirates' Fortress Interior Lower Chest"] = Game.pirates_fortress_access && (Game.goron_mask || Game.hookshot);
	Location_Access["Pirates' Fortress Interior Upper Chest"] = Game.pirates_fortress_access && Game.hookshot;
	Location_Access["Pirates' Fortress Interior Tank Chest"] = Game.pirates_fortress_access && Game.hookshot;
	Location_Access["Pirates' Fortress Interior Guard Room Chest"] = Game.pirates_fortress_access && Game.hookshot;
		
	// Pinnacle Rock
	Location_Access["Seahorses"] = Game.west_access && Game.zora_mask && Game.any_bottle && Game.seahorse;
	Location_Access["Pinnacle Rock Lower Chest"] = Game.pinnacle_rock_access && Game.west_access;
	Location_Access["Pinnacle Rock Upper Chest"] = Game.pinnacle_rock_access && Game.west_access;
		
	// Path to Ikana Canyon
	Location_Access["Invisible Soldier"] = Game.lens && Game.magic && Game.any_bottle && Game.eponas_song;
	Location_Access["Path to Ikana Pillar Chest"] = Game.hookshot;
	Location_Access["Path to Ikana Grotto"] = Game.goron_mask;
		
	// Ikana Graveyard
	Location_Access["Dampe Digging"] = Game.captains_hat && Game.ikana_graveyard_access && (Game.zora_mask || Game.bow);
	Location_Access["Iron Knuckle Chest"] = Game.captains_hat && Game.ikana_graveyard_access && Game.explosive;
	Location_Access["Captain Keeta's Chest"] = Game.sonata && Game.ikana_graveyard_access && (Game.bow || Game.bomb);
	Location_Access["Day 1 Grave Bats"] = Game.captains_hat && Game.ikana_graveyard_access;
	Location_Access["Ikana Graveyard Grotto"] = Game.ikana_graveyard_access && Game.explosive;
		
	// Ikana Canyon
	Location_Access["Poe Hut"] = Game.ikana_canyon_access && (Game.bow || Game.bomb);
	Location_Access["Pamela's Father"] = Game.song_of_healing && Game.ikana_canyon_access && Game.song_of_storms;
	Location_Access["Secret Shrine Grotto"] = Game.east_access;
	Location_Access["Ikana Canyon Ledge"] = Game.east_access;
	Location_Access["Canyon Scrub Trade"] = Game.zora_mask && Game.ocean_title_deed && Game.east_access;
		
	// Secret Shrine
	Location_Access["Secret Shrine Final Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Access["Secret Shrine Dinolfos Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Access["Secret Shrine Wizzrobe Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Access["Secret Shrine Wart Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Access["Secret Shrine Garo Master Chest"] = Game.shoot_light_arrow && Game.east_access;
		
	// Beneath the Well 
	Location_Access["Mirror Shield Chest"] = Game.ikana_canyon_access && (
		(Game.shoot_fire_arrow && Game.shoot_light_arrow) // from castle, have fire arrows
		|| (Game.shoot_light_arrow && Game.gibdo_mask && Game.any_milk && Game.big_poe) // from castle, use the stick to bring fire through
		|| (Game.gibdo_mask && Game.big_poe && Game.any_milk && Game.limitless_magic_beans) // normal well path
	);
	Location_Access["Well Right Path Chest"] =  Game.ikana_canyon_access && Game.gibdo_mask && Game.any_bottle && (
		Game.shoot_light_arrow // from castle
		|| Game.limitless_magic_beans // normal well path
	);
	Location_Access["Well Left Path Chest"] = Game.ikana_canyon_access && Game.gibdo_mask && Game.any_blue_potion;
		
	// Ikana Castle
	Location_Access["Ikana Castle Pillar"] = Game.ikana_canyon_access && (Game.shoot_light_arrow || Game.mirror_shield || Game.zora_mask);
		
	// Stone Tower
	Location_Access["Inverted Stone Tower Left Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;
	Location_Access["Inverted Stone Tower Middle Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;
	Location_Access["Inverted Stone Tower Right Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;	
	
	// Woodfall Temple
	Location_Access["Woodfall Entrance Platform"] = Game.wft_access;
	Location_Access["Woodfall Dark Room"] = Game.wft_access;
	Location_Access["Woodfall Main Room Switch"] = Game.wft_access;
	Location_Access["Hero's Bow Chest"] = Game.wft_access;
	Location_Access["Woodfall Map Chest"] = Game.wft_access;
	Location_Access["Woodfall Small Key Chest"] = Game.wft_access;
	Location_Access["Woodfall Compass Chest"] = Game.wft_access;
	Location_Access["Woodfall Boss Key Chest"] = Game.wft_access && Game.bow;
	Location_Access["Odolwa Heart Container"] = Game.wft_access && Game.bow;
		
	// Snowhead Temple
	// sht_access is goron+bow+lullaby
	Location_Access["Fire Arrow Chest"] = Game.sht_access && (Game.shoot_fire_arrow || (Game.hookshot && Game.magic) || Game.explosive);
	Location_Access["Goht Heart Container"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Access["Snowhead Boss Key Chest"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Access["Snowhead Basement"] = Game.sht_access;
	Location_Access["Snowhead Block Room Chest"] = Game.sht_access && (Game.magic || Game.hookshot || Game.explosive || Game.zora_mask);
	Location_Access["Snowhead Bridge Room Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask);
	Location_Access["Snowhead Compass Chest"] = Game.sht_access;
	Location_Access["Snowhead Ice Puzzle"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.zora_mask);
	Location_Access["Snowhead Icicle Room Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Access["Snowhead Icicle Room Wall"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Access["Snowhead Main Room Wall"] = Game.sht_access && ((Game.hookshot && Game.magic) || Game.shoot_fire_arrow);
	Location_Access["Snowhead Map Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask || Game.magic);
	Location_Access["Snowhead Map Room Ledge"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Access["Snowhead Pillar Freezards"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Access["Snowhead Twin Block"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask);
		
	// Great Bay Temple
	Location_Access["Great Bay Entrance Torches"] = Game.gbt_access;
	Location_Access["Great Bay Bio Babas"] = Game.gbt_access;
	Location_Access["Great Bay Green Valve"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Access["Great Bay Seesaw Room"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Access["Great Bay Waterwheel Room Lower"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Access["Great Bay Waterwheel Room Upper"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Access["Ice Arrow Chest"] = Game.gbt_access;
	Location_Access["Great Bay Boss Key Chest"] = Game.gbt_access;
	Location_Access["Great Bay Map Chest"] = Game.gbt_access;
	Location_Access["Great Bay Compass Chest"] = Game.gbt_access;
	Location_Access["Great Bay Small Key Chest"] = Game.gbt_access;
	Location_Access["Gyorg Heart Container"] = Game.gbt_access && Game.shoot_ice_arrow;
	
	// Stone Tower Temple
	Location_Access["Stone Tower Statue Eye"] = Game.stt_access && Game.bow;
	Location_Access["Stone Tower Compass Chest"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask));
	Location_Access["Stone Tower Underwater"] = Game.istt_access;
	Location_Access["Stone Tower Eyegore Room Chest"] = Game.stt_access && ((Game.shoot_light_arrow && Game.zora_mask) || (Game.explosive && Game.goron_mask));
	Location_Access["Stone Tower Bridge Crystal"] = Game.stt_access && ((Game.shoot_light_arrow && Game.zora_mask) || (Game.explosive && Game.goron_mask));
	Location_Access["Stone Tower Basement Ledge"] = Game.stt_access && ((Game.mirror_shield && Game.explosive && Game.goron_mask) || (Game.explosive && Game.shoot_light_arrow && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Access["Stone Tower Map Chest"] = Game.stt_access && (((Game.mirror_shield || Game.shoot_light_arrow) && Game.explosive && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Access["Stone Tower Armos Room Chest"] = Game.stt_access && ((Game.mirror_shield && Game.explosive && Game.goron_mask) || (Game.explosive && Game.shoot_light_arrow && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Access["Stone Tower Mirror Sun Switch"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Access["Stone Tower Mirror Sun Block"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Access["Stone Tower Lava Room Fire Ring"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive)) && Game.goron_mask && (Game.shoot_light_arrow || Game.deku_mask);
	Location_Access["Stone Tower Lava Room Ledge"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive)) && Game.deku_mask;
	Location_Access["Light Arrow Chest"] = Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Access["Stone Tower Thin Bridge"] = (Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive)) && (Game.explosive || Game.great_fairy_sword)) || (Game.istt_access && Game.deku_mask);
	Location_Access["Stone Tower Eyegore"] = Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Access["Stone Tower Death Armos"] = Game.istt_access && Game.deku_mask;
	
	// Inverted Stone Tower Temple
	Location_Access["Stone Tower Entrance Sun Switch"] = Game.istt_access;
	Location_Access["Stone Tower Updraft Frozen Eye"] = Game.istt_access && Game.shoot_fire_arrow;
	Location_Access["Stone Tower Updraft Fire Ring"] = Game.istt_access && Game.zora_mask && Game.deku_mask;
	Location_Access["Stone Tower Updraft Room Chest"] = Game.istt_access;
	Location_Access["Giant's Mask Chest"] = Game.istt_access;
	Location_Access["Stone Tower Death Armos Maze Chest"] = Game.istt_access && (Game.deku_mask || Game.explosive);
	Location_Access["Stone Tower Wizzrobe"] = Game.istt_access && Game.deku_mask;
	Location_Access["Stone Tower Boss Key Chest"] = Game.istt_access;
	Location_Access["Twinmold Heart Container"] = Game.istt_access;
		
	// Songs
	Location_Access["Starting Song"] = true;
	Location_Access["Boss Blue Warp"] = Game.woodfall_clear || Game.snowhead_clear || Game.great_bay_clear || Game.ikana_clear;
	Location_Access["Romani's Game"] = Game.ranch_day_1_access;
	Location_Access["Day 1 Grave Tablet"] = Game.captains_hat && Game.ikana_graveyard_access;
	Location_Access["Imprisoned Monkey"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Access["Swamp Music Statue"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Access["Baby Goron"] = Game.north_access && Game.goron_mask;
	Location_Access["Baby Zoras"] = Game.zora_egg && Game.zora_mask && Game.west_access;
	Location_Access["Ikana King"] = Game.ikana_canyon_access && Game.shoot_fire_arrow && Game.mirror_shield && (Game.shoot_light_arrow || ((Game.deku_mask || Game.zora_mask) && Game.powder_keg && Game.goron_mask));
	Location_Access["Skull Kid Song"] = (Game.deku_mask && Game.magic) || Game.bow || Game.zora_mask || Game.hookshot || Game.bomb || Game.blast_mask;

	
	// South Clock Town
	Location_Obtain["Clock Tower Entrance"] = true;
	Location_Obtain["South Clock Town Straw Roof Chest"] = true;
	Location_Obtain["South Clock Town Final Day Chest"] = Game.hookshot || (Game.deku_mask && Game.moons_tear);
	Location_Obtain["Clock Town Scrub Trade"] = Game.moons_tear;
	Location_Obtain["Postbox"] = Game.postmans_hat;
		
	// North Clock Town
	Location_Obtain["Bombers' Hide and Seek"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
	Location_Obtain["Clock Town Map Purchase"] = true;
	Location_Obtain["Deku Playground Any Day"] = Game.deku_mask;
	Location_Obtain["Keaton Quiz"] = Game.keaton_mask;
	Location_Obtain["North Clock Town Tree"] = true;
	Location_Obtain["Old Lady"] = true;
	Location_Obtain["Town Great Fairy"] = true;
	Location_Obtain["Town Great Fairy Non-Human"] = Game.deku_mask || Game.goron_mask || Game.zora_mask;
		
	// West Clock Town
	Location_Obtain["Bomb Bag Purchase"] = true;
	Location_Obtain["Big Bomb Bag Purchase"] = true;
	Location_Obtain["Bank Reward #1"] = true;
	Location_Obtain["Postman's Game"] = true;
	Location_Obtain["Rosa Sisters"] = Game.kamaro_mask;
	Location_Obtain["Swordsman's School"] = true;
	Location_Obtain["All-Night Mask Purchase"] = Game.giants_wallet;
	Location_Obtain["Bank Reward #3"] = true;
	Location_Obtain["Bank Reward #2"] = true;
		
	// Laundry Pool
	Location_Obtain["Guru Guru"] = true;
	Location_Obtain["Kafei"] = Game.letter_to_kafei;
	Location_Obtain["Curiosity Shop Man #1"] = Game.letter_to_kafei;
	Location_Obtain["Curiosity Shop Man #2"] = Game.letter_to_kafei;
		
	// East Clock Town
	Location_Obtain["Bombers' Hideout Chest"] = Game.explosive;
	Location_Obtain["East Clock Town Chest"] = true;
	Location_Obtain["Gorman"] = Game.deku_mask && Game.goron_mask && Game.zora_mask && Game.romani_mask;
	Location_Obtain["Honey and Darling Any Day"] = Game.bow || Game.bomb || (Game.deku_mask && Game.magic);
	Location_Obtain["Madame Aroma in Bar"] = Game.special_delivery && Game.kafei_mask;
	Location_Obtain["Madame Aroma in Office"] = true;
	Location_Obtain["Mayor"] = Game.couples_mask;
	Location_Obtain["Postman's Freedom Reward"] = Game.special_delivery;
	Location_Obtain["Town Archery #1"] = Game.bow;
	Location_Obtain["Treasure Chest Game Goron"] = Game.goron_mask;
	Location_Obtain["Milk Bar Milk"] = Game.romani_mask;
	Location_Obtain["Milk Bar Chateau"] = Game.romani_mask && Game.adults_wallet;
	
		
	// Stock Pot Inn
	Location_Obtain["Inn Reservation"] = true;
	Location_Obtain["Midnight Meeting"] = Game.kafei_mask/* && Game.night_inn_access*/; // does require night inn access, but removed that to remind to set up meeting...
	Location_Obtain["Toilet Hand"] = Game.land_title_deed || Game.swamp_title_deed || Game.mountain_title_deed || Game.ocean_title_deed || Game.letter_to_kafei || Game.special_delivery;
	Location_Obtain["Grandma Short Story"] = Game.allnight_mask;
	Location_Obtain["Grandma Long Story"] = Game.allnight_mask;
	Location_Obtain["Inn Staff Room Chest"] = true;
	Location_Obtain["Inn Guest Room Chest"] = Game.room_key;
		
	// Termina Field
	Location_Obtain["Astronomy Telescope"] = true;
	Location_Obtain["Gossip Stones"] = (Game.goron_mask && Game.lullaby) || (Game.deku_mask && Game.sonata && (Game.explosive || Game.goron_mask)) || (Game.zora_mask && Game.nwbn && (Game.explosive || Game.goron_mask));
	Location_Obtain["Business Scrub Purchase"] = Game.adults_wallet;
	Location_Obtain["Peahat Grotto"] = true;
	Location_Obtain["Dodongo Grotto"] = true;
	Location_Obtain["Kamaro"] = Game.song_of_healing;
	Location_Obtain["Termina Field Pillar Grotto"] = true;
	Location_Obtain["Termina Field Grass Grotto"] = true;
	Location_Obtain["Termina Field Underwater Chest"] = Game.zora_mask;
	Location_Obtain["Termina Field Grass Chest"] = true;
	Location_Obtain["Termina Field Stump Chest"] = Game.hookshot || (Game.any_magic_bean && Game.water_for_magic_bean) || (Game.bomb && Game.goron_mask); 
	Location_Obtain["Bio Baba Grotto"] = Game.zora_mask && (Game.explosive || Game.goron_mask);
		
	// Road to Southern Swamp
	Location_Obtain["Swamp Archery #1"] = Game.bow;
	Location_Obtain["Swamp Archery #2"] = Game.bow;
	Location_Obtain["Path to Swamp Grotto"] = true;
	Location_Obtain["Path to Swamp Tree"] = true;
	Location_Obtain["Woodfall Map Purchase"] = true;
	Location_Obtain["Snowhead Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
		
	// Southern Swamp
	Location_Obtain["Koume"] = Game.any_bottle;
	Location_Obtain["Kotake"] = true;
	Location_Obtain["Swamp Scrub Trade"] = Game.land_title_deed;
	Location_Obtain["Pictograph Contest Winner"] = Game.pictobox;
	Location_Obtain["Boat Archery"] = Game.woodfall_clear && Game.any_bottle; 
	
	Location_Obtain["Swamp Spider House Reward"] = Game.poison_swamp_access && (Game.hookshot || Game.zora_mask || (Game.deku_mask && (Game.bow || Game.magic || Game.bomb))) && (Game.hookshot || Game.zora_mask || (Game.deku_mask && Game.water_for_magic_bean)) && Game.any_bottle && (Game.hookshot || Game.zora_mask || (Game.water_for_magic_bean && Game.any_magic_bean && (Game.goron_mask || Game.explosive))) && (Game.hookshot || Game.zora_mask || Game.bow || (Game.deku_mask && Game.magic));
	
	Location_Obtain["Near Swamp Spider House Grotto"] = Game.poison_swamp_access && (Game.deku_mask || Game.zora_mask);
	Location_Obtain["Mystery Woods Grotto"] = true;
	Location_Obtain["Swamp Tourist Center Roof"] = Game.goron_mask || (Game.deku_mask && Game.land_title_deed);
		
	// Deku Palace
	Location_Obtain["Bean Man"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Obtain["Butler"] = Game.woodfall_clear && Game.poison_swamp_access && Game.deku_mask && Game.any_bottle; 
	Location_Obtain["Bean Grotto"] = Game.poison_swamp_access && (((Game.hookshot || Game.water_for_magic_bean) && Game.deku_mask)); 
	Location_Obtain["Deku Palace West Garden"] = Game.poison_swamp_access && Game.deku_mask;
		
	// Woodfall
	Location_Obtain["Woodfall Bridge Chest"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Obtain["Behind Woodfall Owl Chest"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Obtain["Entrance to Woodfall Chest"] = Game.poison_swamp_access && Game.deku_mask;
		
	// Milk Road
	Location_Obtain["Gorman Bros Milk Purchase"] = true;
	Location_Obtain["Gorman Bros Race"] = Game.eponas_song;
	Location_Obtain["Romani Ranch Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
	Location_Obtain["Great Bay Map Purchase"] = (Game.deku_mask && Game.magic) || Game.zora_mask || Game.bow || Game.hookshot;
		
	// Romani Ranch
	Location_Obtain["Aliens Defense"] = Game.ranch_day_1_access && Game.bow;
	Location_Obtain["Dog Race"] = true;
	Location_Obtain["Grog"] = Game.bremen_mask;
	Location_Obtain["Cremia"] = Game.ranch_day_1_access && Game.bow;
	Location_Obtain["Doggy Racetrack Roof Chest"] = true;
		
	// Mountain Village
	Location_Obtain["Mountain Smithy Day 1"] = Game.north_access && Game.adults_wallet && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Obtain["Mountain Smithy Day 2"] = Game.north_access && Game.bottle_gold_dust && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Obtain["Frog Choir"] = Game.dongero_mask && Game.snowhead_clear && Game.wft_access && Game.gbt_access && Game.hookshot && Game.deku_mask && Game.zora_mask && Game.shoot_ice_arrow && Game.shoot_fire_arrow;
	Location_Obtain["Hungry Goron"] = Game.north_access && Game.goron_mask && Game.magic;
	Location_Obtain["Darmani"] = Game.song_of_healing && Game.north_access && Game.lens && Game.magic;
	Location_Obtain["Mountain Waterfall Chest"] = Game.snowhead_clear;
	Location_Obtain["Mountain Spring Grotto"] = Game.snowhead_clear;
		
	// Twin Islands
	Location_Obtain["Goron Race"] = Game.snowhead_clear && Game.goron_mask && Game.magic;
	Location_Obtain["Twin Islands Underwater Ramp Chest"] = Game.snowhead_clear && Game.zora_mask;
	Location_Obtain["Hot Spring Water Grotto"] = Game.north_access && Game.explosive && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
	Location_Obtain["Twin Islands Cave Chest"] = Game.snowhead_clear && Game.zora_mask;
	Location_Obtain["Goron Racetrack Grotto"] = Game.north_access && Game.explosive; // hookshot also needs scarecrow song
		
	// Goron Village
	Location_Obtain["Goron Shop 10 Arrows"] = Game.north_access;
	Location_Obtain["Goron Shop 10 Bombs"] = Game.north_access;
	Location_Obtain["Goron Shop Red Potion"] = Game.north_access;
	Location_Obtain["Powder Keg Challenge"] = Game.north_access && Game.goron_mask && (Game.shoot_fire_arrow || Game.snowhead_clear);
	Location_Obtain["Lens of Truth Chest"] = Game.north_access;
	Location_Obtain["Biggest Bomb Bag Purchase"] = Game.north_access && Game.adults_wallet && (Game.goron_mask || (Game.deku_mask && Game.moons_tear && Game.land_title_deed && Game.swamp_title_deed));
	Location_Obtain["Mountain Scrub Trade"] = Game.north_access && Game.deku_mask && Game.swamp_title_deed;
	Location_Obtain["Lens Cave Invisible Chest"] = Game.north_access;
	Location_Obtain["Lens Cave Rock Chest"] = Game.north_access && Game.explosive;
	Location_Obtain["Goron Village Ledge"] = Game.north_access && Game.deku_mask && Game.swamp_title_deed;
		
	// Path to Snowhead
	Location_Obtain["Path to Snowhead Grotto"] = Game.north_access && Game.explosive && Game.goron_mask;
	Location_Obtain["Path to Snowhead Pillar"] = Game.north_access && Game.goron_mask && Game.hookshot;
		
	// Great Bay Coast
	Location_Obtain["Ocean Spider House Day 1 Reward"] = Game.ocean_skulltulas;
	Location_Obtain["Fisherman Game"] = Game.great_bay_clear && Game.hookshot;
	Location_Obtain["Ocean Spider House Chest"] = Game.bow && Game.ocean_skulltulas;
	Location_Obtain["Mikau"] = Game.west_access && Game.song_of_healing;
	Location_Obtain["Great Bay Coast Grotto"] = Game.west_access;
	Location_Obtain["Lab Fish"] = Game.west_access && Game.any_bottle;
	Location_Obtain["Great Bay Coast Ledge"] = Game.west_access && Game.hookshot && ((Game.any_magic_bean && Game.water_for_magic_bean) || Game.explosive);
	Location_Obtain["Stone Tower Map Purchase"] = Game.west_access && (Game.hookshot || Game.bow);
	Location_Obtain["Fisherman Pictograph"] = Game.pictobox && Game.west_access && Game.pirates_fortress_access;
		
	// Zora Cape
	Location_Obtain["Beaver Race #1"] = Game.hookshot && Game.zora_mask && Game.west_access;
	Location_Obtain["Beaver Race #2"] = Game.hookshot && Game.zora_mask && Game.west_access;
	Location_Obtain["Zora Cape Ledge Without Tree Chest"] = Game.hookshot && Game.west_access;
	Location_Obtain["Zora Cape Ledge With Tree Chest"] = Game.hookshot && Game.west_access;
	Location_Obtain["Zora Cape Grotto"] = Game.west_access && (Game.goron_mask || Game.explosive);
	Location_Obtain["Zora Cape Underwater Chest"] = Game.zora_mask && Game.west_access;
	Location_Obtain["Zora Cape Like-Like"] = Game.west_access && Game.zora_mask;
		
	// Zora Hall
	Location_Obtain["Zora Shop 10 Arrows"] = Game.zora_mask && Game.west_access;
	Location_Obtain["Zora Shop Hero's Shield"] = Game.zora_mask && Game.west_access;
	Location_Obtain["Zora Shop Red Potion"] = Game.zora_mask && Game.west_access;
	Location_Obtain["Ocean Scrub Trade"] = Game.goron_mask && Game.mountain_title_deed && Game.zora_mask && Game.west_access;
	Location_Obtain["Evan"] = Game.zora_mask && Game.west_access;
	Location_Obtain["Lulu's Room Ledge"] = Game.pirates_fortress_access;
	Location_Obtain["Zora Hall Stage Lights"] = Game.shoot_fire_arrow && Game.west_access;
		
	// Pirates' Fortress Exterior
	Location_Obtain["Pirates' Fortress Exterior Log Chest"] = Game.pirates_fortress_access;
	Location_Obtain["Pirates' Fortress Exterior Sand Chest"] = Game.pirates_fortress_access;
	Location_Obtain["Pirates' Fortress Exterior Corner Chest"] = Game.pirates_fortress_access;
		
	// Pirates' Fortress Sewer
	Location_Obtain["Pirates' Fortress Cage Room Shallow Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Obtain["Pirates' Fortress Cage Room Deep Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Obtain["Pirates' Fortress Maze Chest"] = Game.pirates_fortress_access && Game.goron_mask;
	Location_Obtain["Pirates' Fortress Cage"] = Game.pirates_fortress_access && Game.goron_mask;
		
	// Pirates' Fortress Interior
	Location_Obtain["Hookshot Chest"] = Game.pirates_fortress_access && ((Game.bow || (Game.deku_mask && Game.magic)) && (Game.goron_mask || Game.hookshot));
	Location_Obtain["Pirates' Fortress Interior Lower Chest"] = Game.pirates_fortress_access && (Game.goron_mask || Game.hookshot);
	Location_Obtain["Pirates' Fortress Interior Upper Chest"] = Game.pirates_fortress_access && Game.hookshot;
	Location_Obtain["Pirates' Fortress Interior Tank Chest"] = Game.pirates_fortress_access && Game.hookshot;
	Location_Obtain["Pirates' Fortress Interior Guard Room Chest"] = Game.pirates_fortress_access && Game.hookshot;
		
	// Pinnacle Rock
	Location_Obtain["Seahorses"] = Game.west_access && Game.zora_mask && Game.any_bottle && Game.seahorse;
	Location_Obtain["Pinnacle Rock Lower Chest"] = Game.pinnacle_rock_access && Game.west_access;
	Location_Obtain["Pinnacle Rock Upper Chest"] = Game.pinnacle_rock_access && Game.west_access;
		
	// Path to Ikana Canyon
	Location_Obtain["Invisible Soldier"] = Game.lens && Game.magic && Game.any_bottle && Game.eponas_song;
	Location_Obtain["Path to Ikana Pillar Chest"] = Game.hookshot;
	Location_Obtain["Path to Ikana Grotto"] = Game.goron_mask;
		
	// Ikana Graveyard
	Location_Obtain["Dampe Digging"] = Game.captains_hat && Game.ikana_graveyard_access && (Game.zora_mask || Game.bow);
	Location_Obtain["Iron Knuckle Chest"] = Game.captains_hat && Game.ikana_graveyard_access && Game.explosive;
	Location_Obtain["Captain Keeta's Chest"] = Game.sonata && Game.ikana_graveyard_access && (Game.bow || Game.bomb);
	Location_Obtain["Day 1 Grave Bats"] = Game.captains_hat && Game.ikana_graveyard_access;
	Location_Obtain["Ikana Graveyard Grotto"] = Game.ikana_graveyard_access && Game.explosive;
		
	// Ikana Canyon
	Location_Obtain["Poe Hut"] = Game.ikana_canyon_access && (Game.bow || Game.bomb);
	Location_Obtain["Pamela's Father"] = Game.song_of_healing && Game.ikana_canyon_access && Game.song_of_storms;
	/*
	When Link approaches her house, she runs away and goes inside her home, not allowing Link to enter. If Link waits for 90 seconds (in real time) or throws a Bomb near the door, Pamela will leave her house to investigate the well. Link can sneak into the house while Pamela is outside. If Pamela sees him, she will run back into the house and Link must wait until she comes out again
	
	If you have stone mask, just walk in. 
	If you have bombs, get her to go inside, then throw a bomb near the door, then wait for her to come out and go to the well.
	If you don't have bombs, get her to go inside then wait 90 seconds for her to go back outside
	*/
	Location_Obtain["Secret Shrine Grotto"] = Game.east_access;
	Location_Obtain["Ikana Canyon Ledge"] = Game.east_access && Game.zora_mask && Game.ocean_title_deed && Game.deku_mask;
	Location_Obtain["Canyon Scrub Trade"] = Game.zora_mask && Game.ocean_title_deed && Game.east_access;
		
	// Secret Shrine
	Location_Obtain["Secret Shrine Final Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Obtain["Secret Shrine Dinolfos Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Obtain["Secret Shrine Wizzrobe Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Obtain["Secret Shrine Wart Chest"] = Game.shoot_light_arrow && Game.east_access;
	Location_Obtain["Secret Shrine Garo Master Chest"] = Game.shoot_light_arrow && Game.east_access;
		
	// Beneath the Well 
	Location_Obtain["Mirror Shield Chest"] = Game.ikana_canyon_access && (
		(Game.shoot_fire_arrow && Game.shoot_light_arrow) // from castle, have fire arrows
		|| (Game.shoot_light_arrow && Game.gibdo_mask && Game.any_milk && Game.big_poe) // from castle, use the stick to bring fire through
		|| (Game.gibdo_mask && Game.big_poe && Game.any_milk && Game.limitless_magic_beans) // normal well path
	);
	Location_Obtain["Well Right Path Chest"] =  Game.ikana_canyon_access && Game.gibdo_mask && Game.any_bottle && (
		Game.shoot_light_arrow // from castle
		|| Game.limitless_magic_beans // normal well path
	);
	Location_Obtain["Well Left Path Chest"] = Game.ikana_canyon_access && Game.gibdo_mask && Game.any_blue_potion;
		
	// Ikana Castle
	Location_Obtain["Ikana Castle Pillar"] = Game.ikana_canyon_access && Game.shoot_fire_arrow && Game.deku_mask && (Game.mirror_shield || Game.zora_mask || Game.shoot_light_arrow);
		
	// Stone Tower
	Location_Obtain["Inverted Stone Tower Left Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;
	Location_Obtain["Inverted Stone Tower Middle Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;
	Location_Obtain["Inverted Stone Tower Right Chest"] = Game.istt_access && Game.any_magic_bean && Game.water_for_magic_bean;	
	
	// Woodfall Temple
	Location_Obtain["Woodfall Entrance Platform"] = Game.wft_access;
	Location_Obtain["Woodfall Dark Room"] = Game.wft_access;
	Location_Obtain["Woodfall Main Room Switch"] = Game.wft_access;
	Location_Obtain["Hero's Bow Chest"] = Game.wft_access;
	Location_Obtain["Woodfall Map Chest"] = Game.wft_access;
	Location_Obtain["Woodfall Small Key Chest"] = Game.wft_access;
	Location_Obtain["Woodfall Compass Chest"] = Game.wft_access;
	Location_Obtain["Woodfall Boss Key Chest"] = Game.wft_access && Game.bow;
	Location_Obtain["Odolwa Heart Container"] = Game.wft_access && Game.bow;
		
	// Snowhead Temple
	Location_Obtain["Fire Arrow Chest"] = Game.sht_access && (Game.shoot_fire_arrow || (Game.hookshot && Game.magic) || Game.explosive);
	Location_Obtain["Goht Heart Container"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Obtain["Snowhead Boss Key Chest"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Obtain["Snowhead Basement"] = Game.sht_access;
	Location_Obtain["Snowhead Compass Chest"] = Game.sht_access;
	Location_Obtain["Snowhead Block Room Chest"] = Game.sht_access && (Game.magic || Game.hookshot || Game.explosive || Game.zora_mask);
	Location_Obtain["Snowhead Bridge Room Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask);
	Location_Obtain["Snowhead Ice Puzzle"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.zora_mask);
	Location_Obtain["Snowhead Icicle Room Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Obtain["Snowhead Icicle Room Wall"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Obtain["Snowhead Main Room Wall"] = Game.sht_access && ((Game.hookshot && Game.magic) || Game.shoot_fire_arrow);
	Location_Obtain["Snowhead Map Chest"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask || Game.magic);
	Location_Obtain["Snowhead Map Room Ledge"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow);
	Location_Obtain["Snowhead Pillar Freezards"] = Game.sht_access && Game.shoot_fire_arrow;
	Location_Obtain["Snowhead Twin Block"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask);
		
	// Great Bay Temple
	Location_Obtain["Great Bay Entrance Torches"] = Game.gbt_access;
	Location_Obtain["Great Bay Bio Babas"] = Game.gbt_access;
	Location_Obtain["Great Bay Green Valve"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Obtain["Great Bay Seesaw Room"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Obtain["Great Bay Waterwheel Room Lower"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Obtain["Great Bay Waterwheel Room Upper"] = Game.gbt_access && Game.shoot_ice_arrow;
	Location_Obtain["Ice Arrow Chest"] = Game.gbt_access;
	Location_Obtain["Great Bay Boss Key Chest"] = Game.gbt_access;
	Location_Obtain["Great Bay Map Chest"] = Game.gbt_access;
	Location_Obtain["Great Bay Compass Chest"] = Game.gbt_access;
	Location_Obtain["Great Bay Small Key Chest"] = Game.gbt_access;
	Location_Obtain["Gyorg Heart Container"] = Game.gbt_access && Game.shoot_ice_arrow;
	
	// Stone Tower Temple
	Location_Obtain["Stone Tower Statue Eye"] = Game.stt_access && Game.bow;
	Location_Obtain["Stone Tower Compass Chest"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask));
	Location_Obtain["Stone Tower Underwater"] = Game.istt_access && Game.zora_mask;
	Location_Obtain["Stone Tower Eyegore Room Chest"] = Game.stt_access && ((Game.shoot_light_arrow && Game.zora_mask) || (Game.explosive && Game.goron_mask));
	Location_Obtain["Stone Tower Bridge Crystal"] = Game.stt_access && ((Game.shoot_light_arrow && Game.zora_mask) || (Game.explosive && Game.goron_mask && (Game.zora_mask || Game.light_arrow)));
	Location_Obtain["Stone Tower Basement Ledge"] = Game.stt_access && ((Game.mirror_shield && Game.explosive && Game.goron_mask) || (Game.explosive && Game.shoot_light_arrow && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Obtain["Stone Tower Map Chest"] = Game.stt_access && ((Game.explosive && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Obtain["Stone Tower Armos Room Chest"] = Game.stt_access && ((Game.mirror_shield && Game.explosive && Game.goron_mask) || (Game.explosive && Game.shoot_light_arrow && Game.goron_mask) || (Game.shoot_light_arrow && Game.zora_mask));
	Location_Obtain["Stone Tower Mirror Sun Switch"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Obtain["Stone Tower Mirror Sun Block"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Obtain["Stone Tower Lava Room Fire Ring"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive)) && Game.goron_mask && (Game.shoot_light_arrow || Game.deku_mask);
	Location_Obtain["Stone Tower Lava Room Ledge"] = Game.stt_access && (Game.shoot_light_arrow || (Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive)) && Game.deku_mask;
	Location_Obtain["Light Arrow Chest"] = Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Obtain["Stone Tower Thin Bridge"] = (Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && (Game.explosive || Game.great_fairy_sword))) && Game.explosive) || (Game.istt_access && Game.deku_mask);
	Location_Obtain["Stone Tower Eyegore"] = Game.stt_access && (Game.shoot_light_arrow || (Game.deku_mask && Game.mirror_shield && Game.goron_mask && Game.zora_mask && Game.explosive));
	Location_Obtain["Stone Tower Death Armos"] = Game.istt_access && Game.deku_mask;
	
	// Inverted Stone Tower Temple
	Location_Obtain["Stone Tower Entrance Sun Switch"] = Game.istt_access;
	Location_Obtain["Stone Tower Updraft Frozen Eye"] = Game.istt_access && Game.shoot_fire_arrow;
	Location_Obtain["Stone Tower Updraft Fire Ring"] = Game.istt_access && Game.zora_mask && Game.deku_mask;
	Location_Obtain["Stone Tower Updraft Room Chest"] = Game.istt_access;
	Location_Obtain["Giant's Mask Chest"] = Game.istt_access;
	Location_Obtain["Stone Tower Death Armos Maze Chest"] = Game.istt_access && (Game.deku_mask || Game.explosive);
	Location_Obtain["Stone Tower Wizzrobe"] = Game.istt_access && Game.deku_mask;
	Location_Obtain["Stone Tower Boss Key Chest"] = Game.istt_access;
	Location_Obtain["Twinmold Heart Container"] = Game.istt_access;
		
	// Songs
	Location_Obtain["Starting Song"] = true;
	Location_Obtain["Boss Blue Warp"] = Game.woodfall_clear || Game.snowhead_clear || Game.great_bay_clear || Game.ikana_clear;
	Location_Obtain["Romani's Game"] = Game.ranch_day_1_access;
	Location_Obtain["Day 1 Grave Tablet"] = Game.captains_hat && Game.ikana_graveyard_access;
	Location_Obtain["Imprisoned Monkey"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Obtain["Swamp Music Statue"] = Game.poison_swamp_access && Game.deku_mask;
	Location_Obtain["Baby Goron"] = Game.north_access && Game.goron_mask;
	Location_Obtain["Baby Zoras"] = Game.zora_egg && Game.zora_mask && Game.west_access;
	Location_Obtain["Ikana King"] = Game.ikana_canyon_access && Game.shoot_fire_arrow && Game.mirror_shield && (Game.shoot_light_arrow || ((Game.deku_mask || Game.zora_mask) && Game.powder_keg && Game.goron_mask));
	Location_Obtain["Skull Kid Song"] = (Game.deku_mask && Game.magic) || Game.bow || Game.zora_mask || Game.hookshot || Game.bomb || Game.blast_mask;
	
	// South Clock Town
	Location_Could_Obtain["Clock Tower Entrance"] = true;
	Location_Could_Obtain["South Clock Town Straw Roof Chest"] = true;
	Location_Could_Obtain["South Clock Town Final Day Chest"] = CouldHave.hookshot || (CouldHave.deku_mask && CouldHave.moons_tear);
	Location_Could_Obtain["Clock Town Scrub Trade"] = CouldHave.moons_tear;
	Location_Could_Obtain["Postbox"] = CouldHave.postmans_hat;
		
	// North Clock Town
	Location_Could_Obtain["Bombers' Hide and Seek"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
	Location_Could_Obtain["Clock Town Map Purchase"] = true;
	Location_Could_Obtain["Deku Playground Any Day"] = CouldHave.deku_mask;
	Location_Could_Obtain["Keaton Quiz"] = CouldHave.keaton_mask;
	Location_Could_Obtain["North Clock Town Tree"] = true;
	Location_Could_Obtain["Old Lady"] = true;
	Location_Could_Obtain["Town Great Fairy"] = true;
	Location_Could_Obtain["Town Great Fairy Non-Human"] = CouldHave.deku_mask || CouldHave.goron_mask || CouldHave.zora_mask;
		
	// West Clock Town
	Location_Could_Obtain["Bomb Bag Purchase"] = true;
	Location_Could_Obtain["Big Bomb Bag Purchase"] = true;
	Location_Could_Obtain["Bank Reward #1"] = true;
	Location_Could_Obtain["Postman's Game"] = true;
	Location_Could_Obtain["Rosa Sisters"] = CouldHave.kamaro_mask;
	Location_Could_Obtain["Swordsman's School"] = true;
	Location_Could_Obtain["All-Night Mask Purchase"] = CouldHave.giants_wallet;
	Location_Could_Obtain["Bank Reward #3"] = true;
	Location_Could_Obtain["Bank Reward #2"] = true;
		
	// Laundry Pool
	Location_Could_Obtain["Guru Guru"] = true;
	Location_Could_Obtain["Kafei"] = CouldHave.letter_to_kafei;
	Location_Could_Obtain["Curiosity Shop Man #1"] = CouldHave.letter_to_kafei;
	Location_Could_Obtain["Curiosity Shop Man #2"] = CouldHave.letter_to_kafei;
		
	// East Clock Town
	Location_Could_Obtain["Bombers' Hideout Chest"] = CouldHave.explosive;
	Location_Could_Obtain["East Clock Town Chest"] = true;
	Location_Could_Obtain["Gorman"] = CouldHave.deku_mask && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.romani_mask;
	Location_Could_Obtain["Honey and Darling Any Day"] = CouldHave.bow || CouldHave.bomb || (CouldHave.deku_mask && CouldHave.magic);
	Location_Could_Obtain["Madame Aroma in Bar"] = CouldHave.special_delivery && CouldHave.kafei_mask;
	Location_Could_Obtain["Madame Aroma in Office"] = true;
	Location_Could_Obtain["Mayor"] = CouldHave.couples_mask;
	Location_Could_Obtain["Postman's Freedom Reward"] = CouldHave.special_delivery;
	Location_Could_Obtain["Town Archery #1"] = CouldHave.bow;
	Location_Could_Obtain["Treasure Chest CouldHave Goron"] = CouldHave.goron_mask;
	Location_Could_Obtain["Milk Bar Milk"] = CouldHave.romani_mask;
	Location_Could_Obtain["Milk Bar Chateau"] = CouldHave.romani_mask && CouldHave.adults_wallet;
	
		
	// Stock Pot Inn
	Location_Could_Obtain["Inn Reservation"] = true;
	Location_Could_Obtain["Midnight Meeting"] = CouldHave.kafei_mask/* && CouldHave.night_inn_access*/; // does require night inn access, but removed that to remind to set up meeting...
	Location_Could_Obtain["Toilet Hand"] = CouldHave.land_title_deed || CouldHave.swamp_title_deed || CouldHave.mountain_title_deed || CouldHave.ocean_title_deed || CouldHave.letter_to_kafei || CouldHave.special_delivery;
	Location_Could_Obtain["Grandma Short Story"] = CouldHave.allnight_mask;
	Location_Could_Obtain["Grandma Long Story"] = CouldHave.allnight_mask;
	Location_Could_Obtain["Inn Staff Room Chest"] = true;
	Location_Could_Obtain["Inn Guest Room Chest"] = CouldHave.room_key;
		
	// Termina Field
	Location_Could_Obtain["Astronomy Telescope"] = true;
	Location_Could_Obtain["Gossip Stones"] = (CouldHave.goron_mask && CouldHave.lullaby) || (CouldHave.deku_mask && CouldHave.sonata && (CouldHave.explosive || CouldHave.goron_mask)) || (CouldHave.zora_mask && CouldHave.nwbn && (CouldHave.explosive || CouldHave.goron_mask));
	Location_Could_Obtain["Business Scrub Purchase"] = CouldHave.adults_wallet;
	Location_Could_Obtain["Peahat Grotto"] = true;
	Location_Could_Obtain["Dodongo Grotto"] = true;
	Location_Could_Obtain["Kamaro"] = CouldHave.song_of_healing;
	Location_Could_Obtain["Termina Field Pillar Grotto"] = true;
	Location_Could_Obtain["Termina Field Grass Grotto"] = true;
	Location_Could_Obtain["Termina Field Underwater Chest"] = CouldHave.zora_mask;
	Location_Could_Obtain["Termina Field Grass Chest"] = true;
	Location_Could_Obtain["Termina Field Stump Chest"] = CouldHave.hookshot || (CouldHave.any_magic_bean && CouldHave.water_for_magic_bean) || (CouldHave.bomb && CouldHave.goron_mask); 
	Location_Could_Obtain["Bio Baba Grotto"] = CouldHave.zora_mask && (CouldHave.explosive || CouldHave.goron_mask);
		
	// Road to Southern Swamp
	Location_Could_Obtain["Swamp Archery #1"] = CouldHave.bow;
	Location_Could_Obtain["Swamp Archery #2"] = CouldHave.bow;
	Location_Could_Obtain["Path to Swamp Grotto"] = true;
	Location_Could_Obtain["Path to Swamp Tree"] = true;
	Location_Could_Obtain["Woodfall Map Purchase"] = true;
	Location_Could_Obtain["Snowhead Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
		
	// Southern Swamp
	Location_Could_Obtain["Koume"] = CouldHave.any_bottle;
	Location_Could_Obtain["Kotake"] = true;
	Location_Could_Obtain["Swamp Scrub Trade"] = CouldHave.land_title_deed;
	Location_Could_Obtain["Pictograph Contest Winner"] = CouldHave.pictobox;
	Location_Could_Obtain["Boat Archery"] = CouldHave.woodfall_clear && CouldHave.any_bottle; 
	
	Location_Could_Obtain["Swamp Spider House Reward"] = CouldHave.poison_swamp_access && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.deku_mask && (CouldHave.bow || CouldHave.magic || CouldHave.bomb))) && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.deku_mask && CouldHave.water_for_magic_bean)) && CouldHave.any_bottle && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.water_for_magic_bean && CouldHave.any_magic_bean && (CouldHave.goron_mask || CouldHave.explosive))) && (CouldHave.hookshot || CouldHave.zora_mask || CouldHave.bow || (CouldHave.deku_mask && CouldHave.magic));
	
	Location_Could_Obtain["Near Swamp Spider House Grotto"] = CouldHave.poison_swamp_access && (CouldHave.deku_mask || CouldHave.zora_mask);
	Location_Could_Obtain["Mystery Woods Grotto"] = true;
	Location_Could_Obtain["Swamp Tourist Center Roof"] = CouldHave.goron_mask || (CouldHave.deku_mask && CouldHave.land_title_deed);
		
	// Deku Palace
	Location_Could_Obtain["Bean Man"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Obtain["Butler"] = CouldHave.woodfall_clear && CouldHave.poison_swamp_access && CouldHave.deku_mask && CouldHave.any_bottle; 
	Location_Could_Obtain["Bean Grotto"] = CouldHave.poison_swamp_access && (((CouldHave.hookshot || CouldHave.water_for_magic_bean) && CouldHave.deku_mask)); 
	Location_Could_Obtain["Deku Palace West Garden"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
		
	// Woodfall
	Location_Could_Obtain["Woodfall Bridge Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Obtain["Behind Woodfall Owl Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Obtain["Entrance to Woodfall Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
		
	// Milk Road
	Location_Could_Obtain["Gorman Bros Milk Purchase"] = true;
	Location_Could_Obtain["Gorman Bros Race"] = CouldHave.eponas_song;
	Location_Could_Obtain["Romani Ranch Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
	Location_Could_Obtain["Great Bay Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
		
	// Romani Ranch
	Location_Could_Obtain["Aliens Defense"] = CouldHave.ranch_day_1_access && CouldHave.bow;
	Location_Could_Obtain["Dog Race"] = true;
	Location_Could_Obtain["Grog"] = CouldHave.bremen_mask;
	Location_Could_Obtain["Cremia"] = CouldHave.ranch_day_1_access && CouldHave.bow;
	Location_Could_Obtain["Doggy Racetrack Roof Chest"] = true;
		
	// Mountain Village
	Location_Could_Obtain["Mountain Smithy Day 1"] = CouldHave.north_access && CouldHave.adults_wallet && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Obtain["Mountain Smithy Day 2"] = CouldHave.north_access && CouldHave.bottle_gold_dust && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Obtain["Frog Choir"] = CouldHave.dongero_mask && CouldHave.snowhead_clear && CouldHave.wft_access && CouldHave.gbt_access && CouldHave.hookshot && CouldHave.deku_mask && CouldHave.zora_mask && CouldHave.shoot_ice_arrow && CouldHave.shoot_fire_arrow;
	Location_Could_Obtain["Hungry Goron"] = CouldHave.north_access && CouldHave.goron_mask && CouldHave.magic;
	Location_Could_Obtain["Darmani"] = CouldHave.song_of_healing && CouldHave.north_access && CouldHave.lens && CouldHave.magic;
	Location_Could_Obtain["Mountain Waterfall Chest"] = CouldHave.snowhead_clear;
	Location_Could_Obtain["Mountain Spring Grotto"] = CouldHave.snowhead_clear;
		
	// Twin Islands
	Location_Could_Obtain["Goron Race"] = CouldHave.snowhead_clear && CouldHave.goron_mask && CouldHave.magic;
	Location_Could_Obtain["Twin Islands Underwater Ramp Chest"] = CouldHave.snowhead_clear && CouldHave.zora_mask;
	Location_Could_Obtain["Hot Spring Water Grotto"] = CouldHave.north_access && CouldHave.explosive && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Obtain["Twin Islands Cave Chest"] = CouldHave.snowhead_clear && CouldHave.zora_mask;
	Location_Could_Obtain["Goron Racetrack Grotto"] = CouldHave.north_access && CouldHave.explosive; // hookshot also needs scarecrow song
		
	// Goron Village
	Location_Could_Obtain["Goron Shop 10 Arrows"] = CouldHave.north_access;
	Location_Could_Obtain["Goron Shop 10 Bombs"] = CouldHave.north_access;
	Location_Could_Obtain["Goron Shop Red Potion"] = CouldHave.north_access;
	Location_Could_Obtain["Powder Keg Challenge"] = CouldHave.north_access && CouldHave.goron_mask && (CouldHave.shoot_fire_arrow || CouldHave.snowhead_clear);
	Location_Could_Obtain["Lens of Truth Chest"] = CouldHave.north_access;
	Location_Could_Obtain["Biggest Bomb Bag Purchase"] = CouldHave.north_access && CouldHave.adults_wallet && (CouldHave.goron_mask || (CouldHave.deku_mask && CouldHave.moons_tear && CouldHave.land_title_deed && CouldHave.swamp_title_deed));
	Location_Could_Obtain["Mountain Scrub Trade"] = CouldHave.north_access && CouldHave.deku_mask && CouldHave.swamp_title_deed;
	Location_Could_Obtain["Lens Cave Invisible Chest"] = CouldHave.north_access;
	Location_Could_Obtain["Lens Cave Rock Chest"] = CouldHave.north_access && CouldHave.explosive;
	Location_Could_Obtain["Goron Village Ledge"] = CouldHave.north_access && CouldHave.deku_mask && CouldHave.swamp_title_deed;
		
	// Path to Snowhead
	Location_Could_Obtain["Path to Snowhead Grotto"] = CouldHave.north_access && CouldHave.explosive && CouldHave.goron_mask;
	Location_Could_Obtain["Path to Snowhead Pillar"] = CouldHave.north_access && CouldHave.goron_mask && CouldHave.hookshot;
		
	// Great Bay Coast
	Location_Could_Obtain["Ocean Spider House Day 1 Reward"] = CouldHave.ocean_skulltulas;
	Location_Could_Obtain["Fisherman Game"] = CouldHave.great_bay_clear && CouldHave.hookshot;
	Location_Could_Obtain["Ocean Spider House Chest"] = CouldHave.bow && CouldHave.ocean_skulltulas;
	Location_Could_Obtain["Mikau"] = CouldHave.west_access && CouldHave.song_of_healing;
	Location_Could_Obtain["Great Bay Coast Grotto"] = CouldHave.west_access;
	Location_Could_Obtain["Lab Fish"] = CouldHave.west_access && CouldHave.any_bottle;
	Location_Could_Obtain["Great Bay Coast Ledge"] = CouldHave.west_access && CouldHave.hookshot && ((CouldHave.any_magic_bean && CouldHave.water_for_magic_bean) || CouldHave.explosive);
	Location_Could_Obtain["Stone Tower Map Purchase"] = CouldHave.west_access && (CouldHave.hookshot || CouldHave.bow);
	Location_Could_Obtain["Fisherman Pictograph"] = CouldHave.pictobox && CouldHave.west_access && CouldHave.pirates_fortress_access;
		
	// Zora Cape
	Location_Could_Obtain["Beaver Race #1"] = CouldHave.hookshot && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Beaver Race #2"] = CouldHave.hookshot && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Zora Cape Ledge Without Tree Chest"] = CouldHave.hookshot && CouldHave.west_access;
	Location_Could_Obtain["Zora Cape Ledge With Tree Chest"] = CouldHave.hookshot && CouldHave.west_access;
	Location_Could_Obtain["Zora Cape Grotto"] = CouldHave.west_access && (CouldHave.goron_mask || CouldHave.explosive);
	Location_Could_Obtain["Zora Cape Underwater Chest"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Zora Cape Like-Like"] = CouldHave.west_access && CouldHave.zora_mask;
		
	// Zora Hall
	Location_Could_Obtain["Zora Shop 10 Arrows"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Zora Shop Hero's Shield"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Zora Shop Red Potion"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Ocean Scrub Trade"] = CouldHave.goron_mask && CouldHave.mountain_title_deed && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Evan"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Lulu's Room Ledge"] = CouldHave.pirates_fortress_access;
	Location_Could_Obtain["Zora Hall Stage Lights"] = CouldHave.shoot_fire_arrow && CouldHave.west_access;
		
	// Pirates' Fortress Exterior
	Location_Could_Obtain["Pirates' Fortress Exterior Log Chest"] = CouldHave.pirates_fortress_access;
	Location_Could_Obtain["Pirates' Fortress Exterior Sand Chest"] = CouldHave.pirates_fortress_access;
	Location_Could_Obtain["Pirates' Fortress Exterior Corner Chest"] = CouldHave.pirates_fortress_access;
		
	// Pirates' Fortress Sewer
	Location_Could_Obtain["Pirates' Fortress Cage Room Shallow Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Obtain["Pirates' Fortress Cage Room Deep Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Obtain["Pirates' Fortress Maze Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Obtain["Pirates' Fortress Cage"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
		
	// Pirates' Fortress Interior
	Location_Could_Obtain["Hookshot Chest"] = CouldHave.pirates_fortress_access && ((CouldHave.bow || (CouldHave.deku_mask && CouldHave.magic)) && (CouldHave.goron_mask || CouldHave.hookshot));
	Location_Could_Obtain["Pirates' Fortress Interior Lower Chest"] = CouldHave.pirates_fortress_access && (CouldHave.goron_mask || CouldHave.hookshot);
	Location_Could_Obtain["Pirates' Fortress Interior Upper Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
	Location_Could_Obtain["Pirates' Fortress Interior Tank Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
	Location_Could_Obtain["Pirates' Fortress Interior Guard Room Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
		
	// Pinnacle Rock
	Location_Could_Obtain["Seahorses"] = CouldHave.west_access && CouldHave.zora_mask && CouldHave.any_bottle && CouldHave.seahorse;
	Location_Could_Obtain["Pinnacle Rock Lower Chest"] = CouldHave.pinnacle_rock_access && CouldHave.west_access;
	Location_Could_Obtain["Pinnacle Rock Upper Chest"] = CouldHave.pinnacle_rock_access && CouldHave.west_access;
		
	// Path to Ikana Canyon
	Location_Could_Obtain["Invisible Soldier"] = CouldHave.lens && CouldHave.magic && CouldHave.any_bottle && CouldHave.eponas_song;
	Location_Could_Obtain["Path to Ikana Pillar Chest"] = CouldHave.hookshot;
	Location_Could_Obtain["Path to Ikana Grotto"] = CouldHave.goron_mask;
		
	// Ikana Graveyard
	Location_Could_Obtain["Dampe Digging"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access && (CouldHave.zora_mask || CouldHave.bow);
	Location_Could_Obtain["Iron Knuckle Chest"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access && CouldHave.explosive;
	Location_Could_Obtain["Captain Keeta's Chest"] = CouldHave.sonata && CouldHave.ikana_graveyard_access && (CouldHave.bow || CouldHave.bomb);
	Location_Could_Obtain["Day 1 Grave Bats"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access;
	Location_Could_Obtain["Ikana Graveyard Grotto"] = CouldHave.ikana_graveyard_access && CouldHave.explosive;
		
	// Ikana Canyon
	Location_Could_Obtain["Poe Hut"] = CouldHave.ikana_canyon_access && (CouldHave.bow || CouldHave.bomb);
	Location_Could_Obtain["Pamela's Father"] = CouldHave.song_of_healing && CouldHave.ikana_canyon_access && CouldHave.song_of_storms;
	Location_Could_Obtain["Secret Shrine Grotto"] = CouldHave.east_access;
	Location_Could_Obtain["Ikana Canyon Ledge"] = CouldHave.east_access && CouldHave.zora_mask && CouldHave.ocean_title_deed && CouldHave.deku_mask;
	Location_Could_Obtain["Canyon Scrub Trade"] = CouldHave.zora_mask && CouldHave.ocean_title_deed && CouldHave.east_access;
		
	// Secret Shrine
	Location_Could_Obtain["Secret Shrine Final Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Obtain["Secret Shrine Dinolfos Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Obtain["Secret Shrine Wizzrobe Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Obtain["Secret Shrine Wart Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Obtain["Secret Shrine Garo Master Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
		
	// Beneath the Well 
	Location_Could_Obtain["Mirror Shield Chest"] = CouldHave.ikana_canyon_access && (
		(CouldHave.shoot_fire_arrow && CouldHave.shoot_light_arrow) // from castle, have fire arrows
		|| (CouldHave.shoot_light_arrow && CouldHave.gibdo_mask && CouldHave.any_milk && CouldHave.big_poe) // from castle, use the stick to bring fire through
		|| (CouldHave.gibdo_mask && CouldHave.big_poe && CouldHave.any_milk && CouldHave.limitless_magic_beans) // normal well path
	);
	Location_Could_Obtain["Well Right Path Chest"] =  CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.any_bottle && (
		CouldHave.shoot_light_arrow // from castle
		|| CouldHave.limitless_magic_beans // normal well path
	);
	Location_Could_Obtain["Well Left Path Chest"] = CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.any_blue_potion;
		
	// Ikana Castle
	Location_Could_Obtain["Ikana Castle Pillar"] = CouldHave.ikana_canyon_access && CouldHave.shoot_fire_arrow && CouldHave.deku_mask && (CouldHave.mirror_shield || CouldHave.zora_mask || CouldHave.shoot_light_arrow);
		
	// Stone Tower
	Location_Could_Obtain["Inverted Stone Tower Left Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;
	Location_Could_Obtain["Inverted Stone Tower Middle Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;
	Location_Could_Obtain["Inverted Stone Tower Right Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;	
	
	// Woodfall Temple
	Location_Could_Obtain["Woodfall Entrance Platform"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Dark Room"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Main Room Switch"] = CouldHave.wft_access;
	Location_Could_Obtain["Hero's Bow Chest"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Map Chest"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Small Key Chest"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Compass Chest"] = CouldHave.wft_access;
	Location_Could_Obtain["Woodfall Boss Key Chest"] = CouldHave.wft_access && CouldHave.bow;
	Location_Could_Obtain["Odolwa Heart Container"] = CouldHave.wft_access && CouldHave.bow;
		
	// Snowhead Temple
	Location_Could_Obtain["Fire Arrow Chest"] = CouldHave.sht_access && (CouldHave.shoot_fire_arrow || (CouldHave.hookshot && CouldHave.magic) || CouldHave.explosive);
	Location_Could_Obtain["Goht Heart Container"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Obtain["Snowhead Boss Key Chest"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Obtain["Snowhead Basement"] = CouldHave.sht_access;
	Location_Could_Obtain["Snowhead Compass Chest"] = CouldHave.sht_access;
	Location_Could_Obtain["Snowhead Block Room Chest"] = CouldHave.sht_access && (CouldHave.magic || CouldHave.hookshot || CouldHave.explosive || CouldHave.zora_mask);
	Location_Could_Obtain["Snowhead Bridge Room Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask);
	Location_Could_Obtain["Snowhead Ice Puzzle"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.zora_mask);
	Location_Could_Obtain["Snowhead Icicle Room Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Obtain["Snowhead Icicle Room Wall"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Obtain["Snowhead Main Room Wall"] = CouldHave.sht_access && ((CouldHave.hookshot && CouldHave.magic) || CouldHave.shoot_fire_arrow);
	Location_Could_Obtain["Snowhead Map Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask || CouldHave.magic);
	Location_Could_Obtain["Snowhead Map Room Ledge"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Obtain["Snowhead Pillar Freezards"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Obtain["Snowhead Twin Block"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask);
		
	// Great Bay Temple
	Location_Could_Obtain["Great Bay Entrance Torches"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Bio Babas"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Green Valve"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Obtain["Great Bay Seesaw Room"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Obtain["Great Bay Waterwheel Room Lower"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Obtain["Great Bay Waterwheel Room Upper"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Obtain["Ice Arrow Chest"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Boss Key Chest"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Map Chest"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Compass Chest"] = CouldHave.gbt_access;
	Location_Could_Obtain["Great Bay Small Key Chest"] = CouldHave.gbt_access;
	Location_Could_Obtain["Gyorg Heart Container"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	
	// Stone Tower Temple
	Location_Could_Obtain["Stone Tower Statue Eye"] = CouldHave.stt_access && CouldHave.bow;
	Location_Could_Obtain["Stone Tower Compass Chest"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask));
	Location_Could_Obtain["Stone Tower Underwater"] = CouldHave.istt_access && CouldHave.zora_mask;
	Location_Could_Obtain["Stone Tower Eyegore Room Chest"] = CouldHave.stt_access && ((CouldHave.shoot_light_arrow && CouldHave.zora_mask) || (CouldHave.explosive && CouldHave.goron_mask));
	Location_Could_Obtain["Stone Tower Bridge Crystal"] = CouldHave.stt_access && ((CouldHave.shoot_light_arrow && CouldHave.zora_mask) || (CouldHave.explosive && CouldHave.goron_mask && (CouldHave.zora_mask || CouldHave.light_arrow)));
	Location_Could_Obtain["Stone Tower Basement Ledge"] = CouldHave.stt_access && ((CouldHave.mirror_shield && CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.explosive && CouldHave.shoot_light_arrow && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Obtain["Stone Tower Map Chest"] = CouldHave.stt_access && ((CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Obtain["Stone Tower Armos Room Chest"] = CouldHave.stt_access && ((CouldHave.mirror_shield && CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.explosive && CouldHave.shoot_light_arrow && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Obtain["Stone Tower Mirror Sun Switch"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Obtain["Stone Tower Mirror Sun Block"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Obtain["Stone Tower Lava Room Fire Ring"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive)) && CouldHave.goron_mask && (CouldHave.shoot_light_arrow || CouldHave.deku_mask);
	Location_Could_Obtain["Stone Tower Lava Room Ledge"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive)) && CouldHave.deku_mask;
	Location_Could_Obtain["Light Arrow Chest"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Obtain["Stone Tower Thin Bridge"] = (CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && (CouldHave.explosive || CouldHave.great_fairy_sword))) && CouldHave.explosive) || (CouldHave.istt_access && CouldHave.deku_mask);
	Location_Could_Obtain["Stone Tower Eyegore"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Obtain["Stone Tower Death Armos"] = CouldHave.istt_access && CouldHave.deku_mask;
	
	// Inverted Stone Tower Temple
	Location_Could_Obtain["Stone Tower Entrance Sun Switch"] = CouldHave.istt_access;
	Location_Could_Obtain["Stone Tower Updraft Frozen Eye"] = CouldHave.istt_access && CouldHave.shoot_fire_arrow;
	Location_Could_Obtain["Stone Tower Updraft Fire Ring"] = CouldHave.istt_access && CouldHave.zora_mask && CouldHave.deku_mask;
	Location_Could_Obtain["Stone Tower Updraft Room Chest"] = CouldHave.istt_access;
	Location_Could_Obtain["Giant's Mask Chest"] = CouldHave.istt_access;
	Location_Could_Obtain["Stone Tower Death Armos Maze Chest"] = CouldHave.istt_access && (CouldHave.deku_mask || CouldHave.explosive);
	Location_Could_Obtain["Stone Tower Wizzrobe"] = CouldHave.istt_access && CouldHave.deku_mask;
	Location_Could_Obtain["Stone Tower Boss Key Chest"] = CouldHave.istt_access;
	Location_Could_Obtain["Twinmold Heart Container"] = CouldHave.istt_access;
		
	// Songs
	Location_Could_Obtain["Starting Song"] = true;
	Location_Could_Obtain["Boss Blue Warp"] = CouldHave.woodfall_clear || CouldHave.snowhead_clear || CouldHave.great_bay_clear || CouldHave.ikana_clear;
	Location_Could_Obtain["Romani's Game"] = CouldHave.ranch_day_1_access;
	Location_Could_Obtain["Day 1 Grave Tablet"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access;
	Location_Could_Obtain["Imprisoned Monkey"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Obtain["Swamp Music Statue"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Obtain["Baby Goron"] = CouldHave.north_access && CouldHave.goron_mask;
	Location_Could_Obtain["Baby Zoras"] = CouldHave.zora_egg && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Obtain["Ikana King"] = CouldHave.ikana_canyon_access && CouldHave.shoot_fire_arrow && CouldHave.mirror_shield && (CouldHave.shoot_light_arrow || ((CouldHave.deku_mask || CouldHave.zora_mask) && CouldHave.powder_keg && CouldHave.goron_mask));
	Location_Could_Obtain["Skull Kid Song"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.bow || CouldHave.zora_mask || CouldHave.hookshot || CouldHave.bomb || CouldHave.blast_mask;
	
	// South Clock Town
	Location_Could_Peek["Clock Tower Entrance"] = true;
	Location_Could_Peek["South Clock Town Straw Roof Chest"] = true;
	Location_Could_Peek["South Clock Town Final Day Chest"] = CouldHave.hookshot || (CouldHave.deku_mask && CouldHave.moons_tear);
	Location_Could_Peek["Clock Town Scrub Trade"] = CouldHave.moons_tear;
	Location_Could_Peek["Postbox"] = CouldHave.postmans_hat;
		
	// North Clock Town
	Location_Could_Peek["Bombers' Hide and Seek"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
	Location_Could_Peek["Clock Town Map Purchase"] = true;
	Location_Could_Peek["Deku Playground Any Day"] = CouldHave.deku_mask;
	Location_Could_Peek["Keaton Quiz"] = CouldHave.keaton_mask;
	Location_Could_Peek["North Clock Town Tree"] = true;
	Location_Could_Peek["Old Lady"] = true;
	Location_Could_Peek["Town Great Fairy"] = true;
	Location_Could_Peek["Town Great Fairy Non-Human"] = CouldHave.deku_mask || CouldHave.goron_mask || CouldHave.zora_mask;
		
	// West Clock Town
	Location_Could_Peek["Bomb Bag Purchase"] = true;
	Location_Could_Peek["Big Bomb Bag Purchase"] = true;
	Location_Could_Peek["Bank Reward #1"] = true;
	Location_Could_Peek["Postman's Game"] = true;
	Location_Could_Peek["Rosa Sisters"] = CouldHave.kamaro_mask;
	Location_Could_Peek["Swordsman's School"] = true;
	Location_Could_Peek["All-Night Mask Purchase"] = true;
	Location_Could_Peek["Bank Reward #3"] = true;
	Location_Could_Peek["Bank Reward #2"] = true;
		
	// Laundry Pool
	Location_Could_Peek["Guru Guru"] = true;
	Location_Could_Peek["Kafei"] = CouldHave.letter_to_kafei;
	Location_Could_Peek["Curiosity Shop Man #1"] = CouldHave.letter_to_kafei;
	Location_Could_Peek["Curiosity Shop Man #2"] = CouldHave.letter_to_kafei;
		
	// East Clock Town
	Location_Could_Peek["Bombers' Hideout Chest"] = CouldHave.explosive;
	Location_Could_Peek["East Clock Town Chest"] = true;
	Location_Could_Peek["Gorman"] = CouldHave.deku_mask && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.romani_mask;
	Location_Could_Peek["Honey and Darling Any Day"] = CouldHave.bow || CouldHave.bomb || (CouldHave.deku_mask && CouldHave.magic);
	Location_Could_Peek["Madame Aroma in Bar"] = CouldHave.special_delivery && CouldHave.kafei_mask;
	Location_Could_Peek["Madame Aroma in Office"] = true;
	Location_Could_Peek["Mayor"] = CouldHave.couples_mask;
	Location_Could_Peek["Postman's Freedom Reward"] = CouldHave.special_delivery;
	Location_Could_Peek["Town Archery #1"] = CouldHave.bow;
	Location_Could_Peek["Treasure Chest Game Goron"] = CouldHave.goron_mask;
	Location_Could_Peek["Milk Bar Milk"] = CouldHave.romani_mask;
	Location_Could_Peek["Milk Bar Chateau"] = CouldHave.romani_mask;
		
	// Stock Pot Inn
	Location_Could_Peek["Inn Reservation"] = true;
	Location_Could_Peek["Midnight Meeting"] = CouldHave.kafei_mask/* && CouldHave.night_inn_access*/; // does require night inn access, but removed that to remind to set up meeting...
	Location_Could_Peek["Toilet Hand"] = CouldHave.land_title_deed || CouldHave.swamp_title_deed || CouldHave.mountain_title_deed || CouldHave.ocean_title_deed || CouldHave.letter_to_kafei || CouldHave.special_delivery;
	Location_Could_Peek["Grandma Short Story"] = CouldHave.allnight_mask;
	Location_Could_Peek["Grandma Long Story"] = CouldHave.allnight_mask;
	Location_Could_Peek["Inn Staff Room Chest"] = true;
	Location_Could_Peek["Inn Guest Room Chest"] = CouldHave.room_key;
		
	// Termina Field
	Location_Could_Peek["Astronomy Telescope"] = true;
	Location_Could_Peek["Gossip Stones"] = (CouldHave.goron_mask && CouldHave.lullaby) || (CouldHave.deku_mask && CouldHave.sonata && (CouldHave.explosive || CouldHave.goron_mask)) || (CouldHave.zora_mask && CouldHave.nwbn && (CouldHave.explosive || CouldHave.goron_mask));
	Location_Could_Peek["Business Scrub Purchase"] = true;
	Location_Could_Peek["Peahat Grotto"] = true;
	Location_Could_Peek["Dodongo Grotto"] = true;
	Location_Could_Peek["Kamaro"] = CouldHave.song_of_healing;
	Location_Could_Peek["Termina Field Pillar Grotto"] = true;
	Location_Could_Peek["Termina Field Grass Grotto"] = true;
	Location_Could_Peek["Termina Field Underwater Chest"] = CouldHave.zora_mask;
	Location_Could_Peek["Termina Field Grass Chest"] = true;
	Location_Could_Peek["Termina Field Stump Chest"] = CouldHave.hookshot || (CouldHave.any_magic_bean && CouldHave.water_for_magic_bean) || (CouldHave.bomb && CouldHave.goron_mask); 
	Location_Could_Peek["Bio Baba Grotto"] = (CouldHave.zora_mask || CouldHave.hookshot || CouldHave.bow || (CouldHave.deku_mask && CouldHave.magic)) && (CouldHave.explosive || CouldHave.goron_mask);
		
	// Road to Southern Swamp
	Location_Could_Peek["Swamp Archery #1"] = CouldHave.bow;
	Location_Could_Peek["Swamp Archery #2"] = CouldHave.bow;
	Location_Could_Peek["Path to Swamp Grotto"] = true;
	Location_Could_Peek["Path to Swamp Tree"] = true;
	Location_Could_Peek["Woodfall Map Purchase"] = true;
	Location_Could_Peek["Snowhead Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
		
	// Southern Swamp
	Location_Could_Peek["Koume"] = CouldHave.any_bottle;
	Location_Could_Peek["Kotake"] = true;
	Location_Could_Peek["Swamp Scrub Trade"] = CouldHave.land_title_deed;
	Location_Could_Peek["Pictograph Contest Winner"] = CouldHave.pictobox;
	Location_Could_Peek["Boat Archery"] = CouldHave.woodfall_clear && CouldHave.any_bottle; 
	
	Location_Could_Peek["Swamp Spider House Reward"] = CouldHave.poison_swamp_access && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.deku_mask && (CouldHave.bow || CouldHave.magic || CouldHave.bomb))) && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.deku_mask && CouldHave.water_for_magic_bean)) && CouldHave.any_bottle && (CouldHave.hookshot || CouldHave.zora_mask || (CouldHave.water_for_magic_bean && CouldHave.any_magic_bean && (CouldHave.goron_mask || CouldHave.explosive))) && (CouldHave.hookshot || CouldHave.zora_mask || CouldHave.bow || (CouldHave.deku_mask && CouldHave.magic));
	
	Location_Could_Peek["Near Swamp Spider House Grotto"] = CouldHave.poison_swamp_access && (CouldHave.deku_mask || CouldHave.zora_mask);
	Location_Could_Peek["Mystery Woods Grotto"] = true;
	Location_Could_Peek["Swamp Tourist Center Roof"] = true;
		
	// Deku Palace
	Location_Could_Peek["Bean Man"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Peek["Butler"] = CouldHave.woodfall_clear && CouldHave.poison_swamp_access && CouldHave.deku_mask && CouldHave.any_bottle; 
	Location_Could_Peek["Bean Grotto"] = CouldHave.poison_swamp_access && (((CouldHave.hookshot || CouldHave.water_for_magic_bean) && CouldHave.deku_mask)); 
	Location_Could_Peek["Deku Palace West Garden"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
		
	// Woodfall
	Location_Could_Peek["Woodfall Bridge Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Peek["Behind Woodfall Owl Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Peek["Entrance to Woodfall Chest"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
		
	// Milk Road
	Location_Could_Peek["Gorman Bros Milk Purchase"] = true;
	Location_Could_Peek["Gorman Bros Race"] = CouldHave.eponas_song;
	Location_Could_Peek["Romani Ranch Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
	Location_Could_Peek["Great Bay Map Purchase"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.zora_mask || CouldHave.bow || CouldHave.hookshot;
		
	// Romani Ranch
	Location_Could_Peek["Aliens Defense"] = CouldHave.ranch_day_1_access && CouldHave.bow;
	Location_Could_Peek["Dog Race"] = true;
	Location_Could_Peek["Grog"] = CouldHave.bremen_mask;
	Location_Could_Peek["Cremia"] = CouldHave.ranch_day_1_access && CouldHave.bow;
	Location_Could_Peek["Doggy Racetrack Roof Chest"] = true;
		
	// Mountain Village
	Location_Could_Peek["Mountain Smithy Day 1"] = CouldHave.north_access && CouldHave.adults_wallet && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Peek["Mountain Smithy Day 2"] = CouldHave.north_access && CouldHave.bottle_gold_dust && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Peek["Frog Choir"] = CouldHave.dongero_mask && CouldHave.snowhead_clear && CouldHave.wft_access && CouldHave.gbt_access && CouldHave.hookshot && CouldHave.deku_mask && CouldHave.zora_mask && CouldHave.shoot_ice_arrow && CouldHave.shoot_fire_arrow;
	Location_Could_Peek["Hungry Goron"] = CouldHave.north_access && CouldHave.goron_mask && CouldHave.magic;
	Location_Could_Peek["Darmani"] = CouldHave.song_of_healing && CouldHave.north_access && CouldHave.lens && CouldHave.magic;
	Location_Could_Peek["Mountain Waterfall Chest"] = CouldHave.snowhead_clear;
	Location_Could_Peek["Mountain Spring Grotto"] = CouldHave.snowhead_clear;
		
	// Twin Islands
	Location_Could_Peek["Goron Race"] = CouldHave.snowhead_clear && CouldHave.goron_mask && CouldHave.magic;
	Location_Could_Peek["Twin Islands Underwater Ramp Chest"] = CouldHave.snowhead_clear && CouldHave.zora_mask;
	Location_Could_Peek["Hot Spring Water Grotto"] = CouldHave.north_access && CouldHave.explosive && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
	Location_Could_Peek["Twin Islands Cave Chest"] = CouldHave.snowhead_clear && CouldHave.zora_mask;
	Location_Could_Peek["Goron Racetrack Grotto"] = CouldHave.north_access && CouldHave.explosive; // hookshot also needs scarecrow song
		
	// Goron Village
	Location_Could_Peek["Goron Shop 10 Arrows"] = CouldHave.north_access;
	Location_Could_Peek["Goron Shop 10 Bombs"] = CouldHave.north_access;
	Location_Could_Peek["Goron Shop Red Potion"] = CouldHave.north_access;
	Location_Could_Peek["Powder Keg Challenge"] = CouldHave.north_access && CouldHave.goron_mask && (CouldHave.shoot_fire_arrow || CouldHave.snowhead_clear);
	Location_Could_Peek["Lens of Truth Chest"] = CouldHave.north_access;
	Location_Could_Peek["Biggest Bomb Bag Purchase"] = CouldHave.north_access && (CouldHave.goron_mask || CouldHave.deku_mask);
	Location_Could_Peek["Mountain Scrub Trade"] = CouldHave.north_access && CouldHave.deku_mask && CouldHave.swamp_title_deed;
	Location_Could_Peek["Lens Cave Invisible Chest"] = CouldHave.north_access;
	Location_Could_Peek["Lens Cave Rock Chest"] = CouldHave.north_access && CouldHave.explosive;
	Location_Could_Peek["Goron Village Ledge"] = CouldHave.north_access;
		
	// Path to Snowhead
	Location_Could_Peek["Path to Snowhead Grotto"] = CouldHave.north_access && CouldHave.explosive && CouldHave.goron_mask;
	Location_Could_Peek["Path to Snowhead Pillar"] = CouldHave.north_access && CouldHave.goron_mask;
		
	// Great Bay Coast
	Location_Could_Peek["Ocean Spider House Day 1 Reward"] = CouldHave.ocean_skulltulas;
	Location_Could_Peek["Fisherman Game"] = CouldHave.great_bay_clear && CouldHave.hookshot;
	Location_Could_Peek["Ocean Spider House Chest"] = CouldHave.bow && CouldHave.ocean_skulltulas;
	Location_Could_Peek["Mikau"] = CouldHave.west_access && CouldHave.song_of_healing;
	Location_Could_Peek["Great Bay Coast Grotto"] = CouldHave.west_access;
	Location_Could_Peek["Lab Fish"] = CouldHave.west_access && CouldHave.any_bottle;
	Location_Could_Peek["Great Bay Coast Ledge"] = CouldHave.west_access; // roll and c-up to peek. need either (bean and water) or (bomb)
	Location_Could_Peek["Stone Tower Map Purchase"] = CouldHave.west_access && (CouldHave.hookshot || CouldHave.bow);
	Location_Could_Peek["Fisherman Pictograph"] = CouldHave.pictobox && CouldHave.west_access && CouldHave.pirates_fortress_access;
		
	// Zora Cape
	Location_Could_Peek["Beaver Race #1"] = CouldHave.hookshot && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Beaver Race #2"] = CouldHave.hookshot && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Zora Cape Ledge Without Tree Chest"] = CouldHave.hookshot && CouldHave.west_access;
	Location_Could_Peek["Zora Cape Ledge With Tree Chest"] = CouldHave.hookshot && CouldHave.west_access;
	Location_Could_Peek["Zora Cape Grotto"] = CouldHave.west_access && (CouldHave.goron_mask || CouldHave.explosive);
	Location_Could_Peek["Zora Cape Underwater Chest"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Zora Cape Like-Like"] = CouldHave.west_access && (CouldHave.zora_mask || CouldHave.bow || CouldHave.bomb);
		
	// Zora Hall
	Location_Could_Peek["Zora Shop 10 Arrows"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Zora Shop Hero's Shield"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Zora Shop Red Potion"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Ocean Scrub Trade"] = CouldHave.goron_mask && CouldHave.mountain_title_deed && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Evan"] = CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Lulu's Room Ledge"] = CouldHave.pirates_fortress_access;
	Location_Could_Peek["Zora Hall Stage Lights"] = CouldHave.shoot_fire_arrow && CouldHave.west_access;
		
	// Pirates' Fortress Exterior
	Location_Could_Peek["Pirates' Fortress Exterior Log Chest"] = CouldHave.pirates_fortress_access;
	Location_Could_Peek["Pirates' Fortress Exterior Sand Chest"] = CouldHave.pirates_fortress_access;
	Location_Could_Peek["Pirates' Fortress Exterior Corner Chest"] = CouldHave.pirates_fortress_access;
		
	// Pirates' Fortress Sewer
	Location_Could_Peek["Pirates' Fortress Cage Room Shallow Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Peek["Pirates' Fortress Cage Room Deep Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Peek["Pirates' Fortress Maze Chest"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
	Location_Could_Peek["Pirates' Fortress Cage"] = CouldHave.pirates_fortress_access && CouldHave.goron_mask;
		
	// Pirates' Fortress Interior
	Location_Could_Peek["Hookshot Chest"] = CouldHave.pirates_fortress_access && ((CouldHave.bow || (CouldHave.deku_mask && CouldHave.magic)) && (CouldHave.goron_mask || CouldHave.hookshot));
	Location_Could_Peek["Pirates' Fortress Interior Lower Chest"] = CouldHave.pirates_fortress_access && (CouldHave.goron_mask || CouldHave.hookshot);
	Location_Could_Peek["Pirates' Fortress Interior Upper Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
	Location_Could_Peek["Pirates' Fortress Interior Tank Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
	Location_Could_Peek["Pirates' Fortress Interior Guard Room Chest"] = CouldHave.pirates_fortress_access && CouldHave.hookshot;
		
	// Pinnacle Rock
	Location_Could_Peek["Seahorses"] = CouldHave.west_access && CouldHave.zora_mask && CouldHave.any_bottle && CouldHave.seahorse;
	Location_Could_Peek["Pinnacle Rock Lower Chest"] = CouldHave.pinnacle_rock_access && CouldHave.west_access;
	Location_Could_Peek["Pinnacle Rock Upper Chest"] = CouldHave.pinnacle_rock_access && CouldHave.west_access;
		
	// Path to Ikana Canyon
	Location_Could_Peek["Invisible Soldier"] = CouldHave.lens && CouldHave.magic && CouldHave.any_bottle && CouldHave.eponas_song;
	Location_Could_Peek["Path to Ikana Pillar Chest"] = CouldHave.hookshot;
	Location_Could_Peek["Path to Ikana Grotto"] = CouldHave.goron_mask;
		
	// Ikana Graveyard
	Location_Could_Peek["Dampe Digging"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access && (CouldHave.zora_mask || CouldHave.bow);
	Location_Could_Peek["Iron Knuckle Chest"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access && CouldHave.explosive;
	Location_Could_Peek["Captain Keeta's Chest"] = CouldHave.sonata && CouldHave.ikana_graveyard_access && (CouldHave.bow || CouldHave.bomb);
	Location_Could_Peek["Day 1 Grave Bats"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access;
	Location_Could_Peek["Ikana Graveyard Grotto"] = CouldHave.ikana_graveyard_access && CouldHave.explosive;
		
	// Ikana Canyon
	Location_Could_Peek["Poe Hut"] = CouldHave.ikana_canyon_access && (CouldHave.bow || CouldHave.bomb);
	Location_Could_Peek["Pamela's Father"] = CouldHave.song_of_healing && CouldHave.ikana_canyon_access && CouldHave.song_of_storms;
	Location_Could_Peek["Secret Shrine Grotto"] = CouldHave.east_access;
	Location_Could_Peek["Ikana Canyon Ledge"] = CouldHave.east_access;
	Location_Could_Peek["Canyon Scrub Trade"] = CouldHave.zora_mask && CouldHave.ocean_title_deed && CouldHave.east_access;
		
	// Secret Shrine
	Location_Could_Peek["Secret Shrine Final Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Peek["Secret Shrine Dinolfos Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Peek["Secret Shrine Wizzrobe Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Peek["Secret Shrine Wart Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
	Location_Could_Peek["Secret Shrine Garo Master Chest"] = CouldHave.shoot_light_arrow && CouldHave.east_access;
		
	// Beneath the Well 
	Location_Could_Peek["Mirror Shield Chest"] = CouldHave.ikana_canyon_access && (
		(CouldHave.shoot_fire_arrow && CouldHave.shoot_light_arrow) // from castle, have fire arrows
		|| (CouldHave.shoot_light_arrow && CouldHave.gibdo_mask && CouldHave.any_milk && CouldHave.big_poe) // from castle, use the stick to bring fire through
		|| (CouldHave.gibdo_mask && CouldHave.big_poe && CouldHave.any_milk && CouldHave.limitless_magic_beans) // normal well path
	);
	Location_Could_Peek["Well Right Path Chest"] =  CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.any_bottle && (
		CouldHave.shoot_light_arrow // from castle
		|| CouldHave.limitless_magic_beans // normal well path
	);
	Location_Could_Peek["Well Left Path Chest"] = CouldHave.ikana_canyon_access && CouldHave.gibdo_mask && CouldHave.any_blue_potion;
		
	// Ikana Castle
	Location_Could_Peek["Ikana Castle Pillar"] = CouldHave.ikana_canyon_access && (CouldHave.shoot_light_arrow || CouldHave.mirror_shield || CouldHave.zora_mask);
		
	// Stone Tower
	Location_Could_Peek["Inverted Stone Tower Left Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;
	Location_Could_Peek["Inverted Stone Tower Middle Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;
	Location_Could_Peek["Inverted Stone Tower Right Chest"] = CouldHave.istt_access && CouldHave.any_magic_bean && CouldHave.water_for_magic_bean;	
	
	// Woodfall Temple
	Location_Could_Peek["Woodfall Entrance Platform"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Dark Room"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Main Room Switch"] = CouldHave.wft_access;
	Location_Could_Peek["Hero's Bow Chest"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Map Chest"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Small Key Chest"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Compass Chest"] = CouldHave.wft_access;
	Location_Could_Peek["Woodfall Boss Key Chest"] = CouldHave.wft_access && CouldHave.bow;
	Location_Could_Peek["Odolwa Heart Container"] = CouldHave.wft_access && CouldHave.bow;
		
	// Snowhead Temple
	Location_Could_Peek["Fire Arrow Chest"] = CouldHave.sht_access && (CouldHave.shoot_fire_arrow || (CouldHave.hookshot && CouldHave.magic) || CouldHave.explosive);
	Location_Could_Peek["Goht Heart Container"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Peek["Snowhead Boss Key Chest"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Peek["Snowhead Basement"] = CouldHave.sht_access;
	Location_Could_Peek["Snowhead Block Room Chest"] = CouldHave.sht_access && (CouldHave.magic || CouldHave.hookshot || CouldHave.explosive || CouldHave.zora_mask);
	Location_Could_Peek["Snowhead Bridge Room Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask);
	Location_Could_Peek["Snowhead Compass Chest"] = CouldHave.sht_access;
	Location_Could_Peek["Snowhead Ice Puzzle"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.zora_mask);
	Location_Could_Peek["Snowhead Icicle Room Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Peek["Snowhead Icicle Room Wall"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Peek["Snowhead Main Room Wall"] = CouldHave.sht_access && ((CouldHave.hookshot && CouldHave.magic) || CouldHave.shoot_fire_arrow);
	Location_Could_Peek["Snowhead Map Chest"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask || CouldHave.magic);
	Location_Could_Peek["Snowhead Map Room Ledge"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow);
	Location_Could_Peek["Snowhead Pillar Freezards"] = CouldHave.sht_access && CouldHave.shoot_fire_arrow;
	Location_Could_Peek["Snowhead Twin Block"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask);
		
	// Great Bay Temple
	Location_Could_Peek["Great Bay Entrance Torches"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Bio Babas"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Green Valve"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Peek["Great Bay Seesaw Room"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Peek["Great Bay Waterwheel Room Lower"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Peek["Great Bay Waterwheel Room Upper"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	Location_Could_Peek["Ice Arrow Chest"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Boss Key Chest"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Map Chest"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Compass Chest"] = CouldHave.gbt_access;
	Location_Could_Peek["Great Bay Small Key Chest"] = CouldHave.gbt_access;
	Location_Could_Peek["Gyorg Heart Container"] = CouldHave.gbt_access && CouldHave.shoot_ice_arrow;
	
	// Stone Tower Temple
	Location_Could_Peek["Stone Tower Statue Eye"] = CouldHave.stt_access && CouldHave.bow;
	Location_Could_Peek["Stone Tower Compass Chest"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask));
	Location_Could_Peek["Stone Tower Underwater"] = CouldHave.istt_access;
	Location_Could_Peek["Stone Tower Eyegore Room Chest"] = CouldHave.stt_access && ((CouldHave.shoot_light_arrow && CouldHave.zora_mask) || (CouldHave.explosive && CouldHave.goron_mask));
	Location_Could_Peek["Stone Tower Bridge Crystal"] = CouldHave.stt_access && ((CouldHave.shoot_light_arrow && CouldHave.zora_mask) || (CouldHave.explosive && CouldHave.goron_mask));
	Location_Could_Peek["Stone Tower Basement Ledge"] = CouldHave.stt_access && ((CouldHave.mirror_shield && CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.explosive && CouldHave.shoot_light_arrow && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Peek["Stone Tower Map Chest"] = CouldHave.stt_access && (((CouldHave.mirror_shield || CouldHave.shoot_light_arrow) && CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Peek["Stone Tower Armos Room Chest"] = CouldHave.stt_access && ((CouldHave.mirror_shield && CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.explosive && CouldHave.shoot_light_arrow && CouldHave.goron_mask) || (CouldHave.shoot_light_arrow && CouldHave.zora_mask));
	Location_Could_Peek["Stone Tower Mirror Sun Switch"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Peek["Stone Tower Mirror Sun Block"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Peek["Stone Tower Lava Room Fire Ring"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive)) && CouldHave.goron_mask && (CouldHave.shoot_light_arrow || CouldHave.deku_mask);
	Location_Could_Peek["Stone Tower Lava Room Ledge"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive)) && CouldHave.deku_mask;
	Location_Could_Peek["Light Arrow Chest"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Peek["Stone Tower Thin Bridge"] = (CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && (CouldHave.explosive || Game.great_fairy_sword))) && CouldHave.explosive) || (CouldHave.istt_access && CouldHave.deku_mask);
	Location_Could_Peek["Stone Tower Eyegore"] = CouldHave.stt_access && (CouldHave.shoot_light_arrow || (CouldHave.deku_mask && CouldHave.mirror_shield && CouldHave.goron_mask && CouldHave.zora_mask && CouldHave.explosive));
	Location_Could_Peek["Stone Tower Death Armos"] = CouldHave.istt_access && CouldHave.deku_mask;
	
	// Inverted Stone Tower Temple
	Location_Could_Peek["Stone Tower Entrance Sun Switch"] = CouldHave.istt_access;
	Location_Could_Peek["Stone Tower Updraft Frozen Eye"] = CouldHave.istt_access && CouldHave.shoot_fire_arrow;
	Location_Could_Peek["Stone Tower Updraft Fire Ring"] = CouldHave.istt_access && CouldHave.zora_mask && CouldHave.deku_mask;
	Location_Could_Peek["Stone Tower Updraft Room Chest"] = CouldHave.istt_access;
	Location_Could_Peek["Giant's Mask Chest"] = CouldHave.istt_access;
	Location_Could_Peek["Stone Tower Death Armos Maze Chest"] = CouldHave.istt_access && (CouldHave.deku_mask || CouldHave.explosive);
	Location_Could_Peek["Stone Tower Wizzrobe"] = CouldHave.istt_access && CouldHave.deku_mask;
	Location_Could_Peek["Stone Tower Boss Key Chest"] = CouldHave.istt_access;
	Location_Could_Peek["Twinmold Heart Container"] = CouldHave.istt_access;
		
	// Songs
	Location_Could_Peek["Starting Song"] = true;
	Location_Could_Peek["Boss Blue Warp"] = CouldHave.woodfall_clear || CouldHave.snowhead_clear || CouldHave.great_bay_clear || CouldHave.ikana_clear;
	Location_Could_Peek["Romani's Game"] = CouldHave.ranch_day_1_access;
	Location_Could_Peek["Day 1 Grave Tablet"] = CouldHave.captains_hat && CouldHave.ikana_graveyard_access;
	Location_Could_Peek["Imprisoned Monkey"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Peek["Swamp Music Statue"] = CouldHave.poison_swamp_access && CouldHave.deku_mask;
	Location_Could_Peek["Baby Goron"] = CouldHave.north_access && CouldHave.goron_mask;
	Location_Could_Peek["Baby Zoras"] = CouldHave.zora_egg && CouldHave.zora_mask && CouldHave.west_access;
	Location_Could_Peek["Ikana King"] = CouldHave.ikana_canyon_access && CouldHave.shoot_fire_arrow && CouldHave.mirror_shield && (CouldHave.shoot_light_arrow || ((CouldHave.deku_mask || CouldHave.zora_mask) && CouldHave.powder_keg && CouldHave.goron_mask));
	Location_Could_Peek["Skull Kid Song"] = (CouldHave.deku_mask && CouldHave.magic) || CouldHave.bow || CouldHave.zora_mask || CouldHave.hookshot || CouldHave.bomb || CouldHave.blast_mask;
	
	
	// CSMC changes
	if(document.getElementById("settings_option").value != "SCRUBS") {
		Location_Access["South Clock Town Final Day Chest"] = true;
		Location_Access["Bombers' Hideout Chest"] = true;
		Location_Access["Termina Field Underwater Chest"] = true;
		Location_Access["Termina Field Stump Chest"] = true; 
		Location_Access["Bean Grotto"] = Game.poison_swamp_access && Game.deku_mask; 
		Location_Access["Hot Spring Water Grotto"] = Game.north_access && (Game.shoot_fire_arrow || Game.hot_spring_water || Game.snowhead_clear);
		Location_Access["Twin Islands Underwater Ramp Chest"] = Game.north_access;
		Location_Access["Twin Islands Cave Chest"] = Game.snowhead_clear;
		Location_Access["Lens Cave Rock Chest"] = Game.north_access;
		Location_Access["Zora Cape Ledge Without Tree Chest"] = Game.west_access;
		Location_Access["Zora Cape Underwater Chest"] = Game.west_access;
		Location_Access["Pirates' Fortress Interior Upper Chest"] = Game.pirates_fortress_access && (Game.hookshot || Game.goron_mask);
		Location_Access["Path to Ikana Pillar Chest"] = true;
		Location_Access["Captain Keeta's Chest"] = Game.ikana_graveyard_access;
		Location_Access["Snowhead Main Room Wall"] = Game.sht_access && ((Game.hookshot && Game.magic) || Game.shoot_fire_arrow || Game.explosive);
		Location_Access["Snowhead Map Room Ledge"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.shoot_fire_arrow || Game.zora_mask);
		Location_Access["Snowhead Twin Block"] = Game.sht_access && (Game.hookshot || Game.explosive || Game.magic || Game.zora_mask);
		Location_Access["Snowhead Ice Puzzle"] = Game.sht_access;
		Location_Access["Stone Tower Compass Chest"] = Game.stt_access;
		Location_Access["Stone Tower Map Chest"] = Game.stt_access && ((Game.explosive && Game.goron_mask) || (Game.zora_mask && Game.shoot_light_arrow));
		Location_Access["Stone Tower Eyegore Room Chest"] = Game.stt_access && (Game.istt_access || (Game.shoot_light_arrow && Game.zora_mask) || (Game.explosive && Game.goron_mask));
		Location_Access["Stone Tower Death Armos Maze Chest"] = Game.stt_access || (Game.istt_access && (Game.deku_mask || Game.explosive));
		
		Location_Could_Peek["South Clock Town Final Day Chest"] = true;
		Location_Could_Peek["Bombers' Hideout Chest"] = true;
		Location_Could_Peek["Termina Field Underwater Chest"] = true;
		Location_Could_Peek["Termina Field Stump Chest"] = true; 
		Location_Could_Peek["Bean Grotto"] = CouldHave.poison_swamp_access && CouldHave.deku_mask; 
		Location_Could_Peek["Hot Spring Water Grotto"] = CouldHave.north_access && (CouldHave.shoot_fire_arrow || CouldHave.hot_spring_water || CouldHave.snowhead_clear);
		Location_Could_Peek["Twin Islands Underwater Ramp Chest"] = CouldHave.north_access;
		Location_Could_Peek["Twin Islands Cave Chest"] = CouldHave.snowhead_clear;
		Location_Could_Peek["Lens Cave Rock Chest"] = CouldHave.north_access;
		Location_Could_Peek["Zora Cape Ledge Without Tree Chest"] = CouldHave.west_access;
		Location_Could_Peek["Zora Cape Underwater Chest"] = CouldHave.west_access;
		Location_Could_Peek["Pirates' Fortress Interior Upper Chest"] = CouldHave.pirates_fortress_access && (CouldHave.hookshot || CouldHave.goron_mask);
		Location_Could_Peek["Path to Ikana Pillar Chest"] = true;
		Location_Could_Peek["Captain Keeta's Chest"] = CouldHave.ikana_graveyard_access;
		Location_Could_Peek["Snowhead Main Room Wall"] = CouldHave.sht_access && ((CouldHave.hookshot && CouldHave.magic) || CouldHave.shoot_fire_arrow || CouldHave.explosive);
		Location_Could_Peek["Snowhead Twin Block"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.magic || CouldHave.zora_mask);
		Location_Could_Peek["Snowhead Map Room Ledge"] = CouldHave.sht_access && (CouldHave.hookshot || CouldHave.explosive || CouldHave.shoot_fire_arrow || CouldHave.zora_mask);
		Location_Could_Peek["Snowhead Ice Puzzle"] = CouldHave.sht_access;
		Location_Could_Peek["Stone Tower Compass Chest"] = CouldHave.stt_access;
		Location_Could_Peek["Stone Tower Map Chest"] = CouldHave.stt_access && ((CouldHave.explosive && CouldHave.goron_mask) || (CouldHave.zora_mask && CouldHave.shoot_light_arrow));
		Location_Could_Peek["Stone Tower Eyegore Room Chest"] = CouldHave.stt_access && (CouldHave.istt_access || (CouldHave.shoot_light_arrow && CouldHave.zora_mask) || (CouldHave.explosive && CouldHave.goron_mask));
		Location_Could_Peek["Stone Tower Death Armos Maze Chest"] = CouldHave.stt_access || (CouldHave.istt_access && (CouldHave.deku_mask || CouldHave.explosive));
	}
}
