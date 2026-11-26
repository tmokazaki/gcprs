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

/// Tokenize Japanese text with the embedded IPADIC dictionary.
pub fn tokenize(text: &str) -> Result<Vec<Token>> {
    let dictionary = load_dictionary("embedded://ipadic")?;
    let segmenter = Segmenter::new(Mode::Normal, dictionary, None);

    let mut tokens = segmenter.segment(Cow::Borrowed(text))?;
    Ok(tokens
        .iter_mut()
        .map(|token| {
            let text = token.surface.to_string();
            let details = token.details().iter().map(|d| d.to_string()).collect();
            Token { text, details }
        })
        .collect())
}

pub async fn handle(targs: TextArgs) -> Result<()> {
    match targs.text_sub_command {
        TextSubCommand::Tokenize(args) => {
            let ts = tokenize(&args.text)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_japanese() {
        let tokens = tokenize("東京都に住んでいます").unwrap();
        let surfaces: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(
            surfaces,
            vec!["東京", "都", "に", "住ん", "で", "い", "ます"]
        );

        // details: 品詞,品詞細分類1,品詞細分類2,品詞細分類3,活用型,活用形,原形,読み,発音
        let tokyo = &tokens[0];
        assert_eq!(tokyo.details.len(), 9);
        assert_eq!(tokyo.details[0], "名詞");
        assert_eq!(tokyo.details[1], "固有名詞");
        assert_eq!(tokyo.details[7], "トウキョウ");

        let sumu = &tokens[3];
        assert_eq!(sumu.details[0], "動詞");
        assert_eq!(sumu.details[6], "住む");
    }

    #[test]
    fn test_tokenize_empty_text() {
        assert!(tokenize("").unwrap().is_empty());
    }

    #[test]
    fn test_token_table_view() {
        let token = Token {
            text: "東京".to_string(),
            details: vec!["名詞".to_string(), "固有名詞".to_string()],
        };
        assert_eq!(token.columns().len(), 2);
        assert_eq!(
            token.values(),
            vec!["東京".to_string(), "名詞,固有名詞".to_string()]
        );
    }
}
