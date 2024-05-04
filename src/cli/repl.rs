use nu_ansi_term::{Color, Style};
use reedline::{
    default_emacs_keybindings, DefaultCompleter, DefaultHinter, DescriptionMode, EditCommand,
    Emacs, ExampleHighlighter, FileBackedHistory, IdeMenu, KeyCode, KeyModifiers, Keybindings,
    MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu,
};

use super::super::common::config::get_user_path;
use super::token::VALID_TOKENS;

fn add_menu_keybindings(keybindings: &mut Keybindings) {
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );
    keybindings.add_binding(
        KeyModifiers::ALT,
        KeyCode::Enter,
        ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
    );
}

pub fn get_repl() -> anyhow::Result<Reedline> {
    // Min width of the completion box, including the border
    let min_completion_width: u16 = 0;
    // Max width of the completion box, including the border
    let max_completion_width: u16 = 50;
    // Max height of the completion box, including the border
    let max_completion_height: u16 = u16::MAX;
    // Padding inside of the completion box (on the left and right side)
    let padding: u16 = 0;
    // Whether to draw the default border around the completion box
    let border: bool = false;
    // Offset of the cursor from the top left corner of the completion box
    // By default the top left corner is below the cursor
    let cursor_offset: i16 = 0;
    // How the description should be aligned
    let description_mode: DescriptionMode = DescriptionMode::PreferRight;
    // Min width of the description box, including the border
    let min_description_width: u16 = 0;
    // Max width of the description box, including the border
    let max_description_width: u16 = 50;
    // Distance between the completion and the description box
    let description_offset: u16 = 1;
    // If true, the cursor pos will be corrected, so the suggestions match up with the typed text
    // ```text
    // C:\> str
    //      str join
    //      str trim
    //      str split
    // ```
    // If a border is being used
    let correct_cursor_pos: bool = false;
    let commands: Vec<String> = VALID_TOKENS.iter().map(|v| v.to_string()).collect();
    let completer = Box::new(DefaultCompleter::new_with_wordlen(commands.clone(), 2));

    // Use the interactive menu to select options from the completer
    let mut ide_menu = IdeMenu::default()
        .with_name("completion_menu")
        .with_min_completion_width(min_completion_width)
        .with_max_completion_width(max_completion_width)
        .with_max_completion_height(max_completion_height)
        .with_padding(padding)
        .with_cursor_offset(cursor_offset)
        .with_description_mode(description_mode)
        .with_min_description_width(min_description_width)
        .with_max_description_width(max_description_width)
        .with_description_offset(description_offset)
        .with_correct_cursor_pos(correct_cursor_pos);

    if border {
        ide_menu = ide_menu.with_default_border();
    }

    let completion_menu = Box::new(ide_menu);

    let mut keybindings = default_emacs_keybindings();
    add_menu_keybindings(&mut keybindings);

    let edit_mode = Box::new(Emacs::new(keybindings));

    let history = Box::new(
        FileBackedHistory::with_file(5, get_user_path().unwrap().join("plumedb-history.txt"))
            .expect("Error configuring history with file"),
    );
    let hinter =
        Box::new(DefaultHinter::default().with_style(Style::new().italic().fg(Color::DarkGray)));

    let mut highlighter = Box::new(ExampleHighlighter::new(commands));
    highlighter.change_colors(Color::Yellow, Color::White, Color::White);

    let line_editor = Reedline::create()
        .with_completer(completer)
        .with_hinter(hinter)
        .with_history(history)
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_edit_mode(edit_mode)
        .with_highlighter(highlighter);

    Ok(line_editor)
}
