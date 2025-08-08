use crate::common::{render, OutputFormat, TableView};
use anyhow::Result;
use clap::{Args, Subcommand};
use lindera::{
    dictionary::DictionaryConfig,
    mode::Mode,
    tokenizer::{Tokenizer, TokenizerConfig},
    DictionaryKind,
};
use serde::{Deserialize, Serialize};

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
    let dictionary = DictionaryConfig {
        kind: Some(DictionaryKind::IPADIC),
        path: None,
    };
    let config = TokenizerConfig {
        dictionary,
        user_dictionary: None,
        mode: Mode::Normal,
    };
    match targs.text_sub_command {
        TextSubCommand::Tokenize(args) => {
            let tokenizer = Tokenizer::from_config(config)?;

            let mut tokens = tokenizer.tokenize(&args.text)?;
            let ts: Vec<Token> = tokens
                .iter_mut()
                .map(|token| {
                    let text = token.text.to_string();
                    let details = if let Some(details) = token.get_details() {
                        details.iter().map(|d| d.to_string()).collect()
                    } else {
                        vec![]
                    };
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
