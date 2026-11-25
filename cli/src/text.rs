use crate::common::{render, OutputFormat, TableView};
use anyhow::Result;
use clap::{Args, Subcommand};
use lindera::{dictionary::load_dictionary, mode::Mode, segmenter::Segmenter};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Args)]
pub struct TextArgs {
    /// Output raw JSON
    #[clap(short = 'r', long = "raw_json", default_value = "false")]
    pub raw: bool,

    #[clap(subcommand)]
    pub text_sub_command: TextSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum TextSubCommand {
    /// Tokenize text
    Tokenize(TokenizeArgs),
}

#[derive(Default, Debug, Args)]
pub struct TokenizeArgs {
    /// text
    #[clap(short = 't', long = "text")]
    text: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Token {
    text: String,
    details: Vec<String>, //表層形\t品詞,品詞細分類1,品詞細分類2,品詞細分類3,活用型,活用形,原形,読み,発音
}

impl TableView for Token {
    fn columns(&self) -> Vec<String> {
        vec![
            "text".to_string(),
            "details(品詞,品詞細分類1,品詞細分類2,品詞細分類3,活用型,活用形,原形,読み,発音)"
                .to_string(),
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![self.text.clone(), self.details.join(",")]
    }
}

pub async fn handle(targs: TextArgs) -> Result<()> {
    match targs.text_sub_command {
        TextSubCommand::Tokenize(args) => {
            let dictionary = load_dictionary("embedded://ipadic")?;
            let segmenter = Segmenter::new(Mode::Normal, dictionary, None);

            let mut tokens = segmenter.segment(Cow::Borrowed(args.text.as_str()))?;
            let ts: Vec<Token> = tokens
                .iter_mut()
                .map(|token| {
                    let text = token.surface.to_string();
                    let details = token.details().iter().map(|d| d.to_string()).collect();
                    Token { text, details }
                })
                .collect();
            render(
                &ts,
                if targs.raw {
                    OutputFormat::Json
                } else {
                    OutputFormat::Csv
                },
                false,
            )
        }
    }
}
