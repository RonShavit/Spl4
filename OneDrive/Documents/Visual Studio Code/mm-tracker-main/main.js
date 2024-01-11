function Update() {	
	process_inputs();
	woth_and_barren_processing();
	update_item_logic();
	update_location_logic();
	update_checks();
	handleAlternateHintInput();
	handle_item_highlights();
	update_summary_text();
	update_settings();
}
