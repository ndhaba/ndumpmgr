use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Write},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use compress_tools::uncompress_archive;
use log::debug;
use regex::Regex;
use tempfile::{NamedTempFile, TempDir};
use ureq::{Agent, Body, ResponseExt, http::Response};
use visdom::{Vis, types::Elements};

use super::{Error, Result, ResultUtils};
use crate::GameConsole;

trait ResponseUtils {
    fn content_type(&self) -> String;
    fn content_length(&self) -> usize;
}
impl ResponseUtils for Response<Body> {
    fn content_type(&self) -> String {
        self.headers()
            .get("Content-Type")
            .unwrap()
            .to_str()
            .unwrap()
            .split(";")
            .next()
            .unwrap()
            .to_string()
    }
    fn content_length(&self) -> usize {
        self.headers()
            .get("Content-Length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap()
    }
}

#[allow(unused)]
pub(super) struct DatafileLink {
    pub name: String,
    pub link: Option<String>,
    pub last_updated: DateTime<Utc>,
}

fn load_html<'a>(
    agent: &Agent,
    url: &str,
    form_body: Option<HashMap<String, String>>,
) -> Result<(Elements<'a>, String)> {
    let mut response = match form_body {
        Some(body) => agent
            .post(url)
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            )
            .send_form(body)
            .catalog("Failed to connect to No-Intro")?,
        None => agent
            .get(url)
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            )
            .call()
            .catalog("Failed to connect to No-Intro")?,
    };
    if !response.status().is_success() {
        return Err(Error::new_original(format!(
            "Failed to connect to No-Intro\n{}",
            response.status()
        )));
    }
    if response.content_type() != "text/html" {
        return Err(Error::new_original(
            "Failed to connect to No-Intro\nNot HTML",
        ));
    }
    let elements = Vis::load(
        response
            .body_mut()
            .read_to_string()
            .catalog("Failed to connect to No-Intro")?,
    )
    .catalog("Failed to connect to No-Intro")?;
    Ok((elements, response.get_uri().to_string()))
}

fn get_form_data(form: &Elements, submit_selector: &str) -> Result<HashMap<String, String>> {
    if form.length() != 1 {
        return Err(Error::new_original(format!(
            "Failed to download No-Intro datafile\nExpected 1 <form> element to extract data, got {}",
            form.length()
        )));
    }
    let mut form_data = HashMap::new();
    for select in form.find("select") {
        let name = select
            .get_attribute("name")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        let selected_option = select.children().filter("option[selected]");
        if selected_option.length() == 0 {
            return Err(Error::new_original(
                "Failed to download No-Intro datafile\n<select> node missing checked <option> node",
            ));
        } else if selected_option.length() != 1 {
            return Err(Error::new_original(
                "Failed to download No-Intro datafile\n<select> node has multiple checked <option> node",
            ));
        }
        let value = selected_option
            .attr("value")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        form_data.insert(name, value);
    }
    for checked_item in form.find("input[checked]") {
        let name = checked_item
            .get_attribute("name")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        let value = checked_item
            .get_attribute("value")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        form_data.insert(name, value);
    }
    let submit_buttons = form.find(submit_selector);
    if submit_buttons.length() != 1 {
        return Err(Error::new_original(format!(
            "Failed to download No-Intro datafile\nExpected 1 submit button to extract data, got {}",
            submit_buttons.length()
        )));
    }
    for submit_button in submit_buttons {
        let name = submit_button
            .get_attribute("name")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        let value = submit_button
            .get_attribute("value")
            .catalog("Failed to download No-Intro datafile")?
            .to_string();
        form_data.insert(name, value);
    }
    Ok(form_data)
}

fn download_datafile_zip(agent: &Agent, link: &str) -> Result<NamedTempFile> {
    let mut file =
        NamedTempFile::with_suffix(".zip").catalog("Failed to download No-Intro datafile")?;
    // go to the datafile configuration settings
    let (root, url) = load_html(agent, link, None)?;
    // prepare the datafile
    let form_data = get_form_data(
        &root.find("form[name=\"main_form\"]"),
        "input[type=\"submit\"][value=\"Prepare\"]",
    )?;
    let (root, url) = load_html(agent, url.as_ref(), Some(form_data))?;
    // download the file
    let form_data = get_form_data(
        &root.find(".standard form"),
        "input[type=\"submit\"][value=\"Download!!\"]",
    )?;
    let mut response = agent
        .post(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        )
        .send_form(form_data)
        .catalog("Failed to download No-Intro datafile")?;
    if response.content_type() != "application/zip" {
        return Err(Error::new_original(format!(
            "Failed to download No-Intro datafile\nExpected \"application/json\" response, got {}",
            response.content_type()
        )));
    }
    // save the downloaded file
    let len: usize = response.content_length();
    let body = response.body_mut();
    let mut bytes = Vec::with_capacity(len);
    body.as_reader()
        .read_to_end(&mut bytes)
        .catalog("Failed to download No-Intro datafile")?;
    file.write(&bytes)
        .catalog("Failed to download No-Intro datafile")?;
    Ok(file)
}

fn extract_datafile(file: &NamedTempFile) -> Result<String> {
    let folder = TempDir::new().catalog("Failed to extract zip")?;
    uncompress_archive(
        BufReader::new(file),
        folder.path(),
        compress_tools::Ownership::Ignore,
    )
    .catalog("Failed to extract zip")?;
    debug!(
        "Extracted zipped datafile to \"{}\"",
        folder.path().to_str().unwrap()
    );
    let mut file = 'file_find: {
        for file in folder
            .path()
            .read_dir()
            .catalog("Failed to find downloaded datafile")?
        {
            let path = file.catalog("Failed to find downloaded datafile")?.path();
            if let Some(extension) = path.extension() {
                if extension == "dat" {
                    break 'file_find File::open(path).catalog("Failed to open datafile")?;
                }
            }
        }
        return Err(Error::new_original(
            "Failed to find downloaded datafile.\nNot included in the download",
        ));
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .catalog("Failed to read datafile")?;
    Ok(contents)
}

pub(super) fn load_datafile_links(agent: &Agent) -> Result<HashMap<String, DatafileLink>> {
    let time_regex = Regex::new(r"(?<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap();
    let (page, _) = load_html(
        agent,
        "https://datomatic.no-intro.org/index.php?page=download&s=64&op=select",
        None,
    )?;
    let no_intro_table = page.find(".info-table").filter_by(|_, elem| {
        elem.children()
            .parent("")
            .find(".discussion_section")
            .text()
            .trim()
            == "No-Intro"
    });
    if no_intro_table.length() != 1 {
        return Err(Error::new_original(format!(
            "Failed to load No-Intro datafile status\n{} No-Intro tables",
            no_intro_table.length()
        )));
    }
    let mut links = HashMap::new();
    for element in no_intro_table.find("tr:not(.discussion_section,.titlef)") {
        let children = element.children();
        let last_td = children.last();
        let name = last_td.find("b").text().trim().to_string();
        links.insert(
            name.clone(),
            DatafileLink {
                name,
                link: {
                    let a = children.first().find("a");
                    if a.has_attr("href") {
                        Some(format!(
                            "https://datomatic.no-intro.org/{}",
                            a.attr("href")
                                .catalog("Failed to load No-Intro datafile status\nMissing link")?
                                .to_string()
                        ))
                    } else {
                        None
                    }
                },
                last_updated: NaiveDateTime::parse_from_str(
                    &time_regex.captures(&last_td.text()).unwrap()["time"],
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap()
                .and_utc(),
            },
        );
    }
    Ok(links)
}

pub(super) fn download_datafile(agent: &Agent, url: &str) -> Result<String> {
    extract_datafile(&download_datafile_zip(agent, url)?)
}

impl GameConsole {
    pub(super) fn nointro_datafile_name(&self) -> Option<&str> {
        match self {
            Self::GB => Some("Nintendo - Game Boy"),
            Self::GBA => Some("Nintendo - Game Boy Advance"),
            Self::GBC => Some("Nintendo - Game Boy Color"),
            Self::N64 => Some("Nintendo - Nintendo 64"),
            _ => None,
        }
    }
}
