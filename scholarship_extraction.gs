/**
 * Gmail → Sheet via LLM extraction
 *
 * Sheet: Scholarships to Validate
 * Label: CSPScholarships
 *
 * Columns written (Row 1 must match exactly):
 * Title | Sponsor | Amount | Closing Date | Description | Link | How to Apply | Eligibility | Type | Location | Application Window
 *
 * Provider: choose 'openai' OR 'anthropic' OR 'gemini'
 *   - Add API key in Script Properties:
 *       OPENAI_API_KEY  or  ANTHROPIC_API_KEY  or  GEMINI_API_KEY
 */

const PROVIDER = 'openai'; // <-- 'openai' | 'anthropic' | 'gemini'
const MODEL_OPENAI = 'gpt-4o-mini';         // fast & accurate for extraction
const MODEL_ANTHROPIC = 'claude-3-5-sonnet-20240620';
const MODEL_GEMINI = 'gemini-1.5-pro';

const LABEL_NAME = 'CSPScholarships';
const SHEET_NAME = 'Scholarships to Validate';
const PROCESSED_SHEET = '_processed_ids';

const HEADERS = [
  'Title','Sponsor','Amount','Closing Date','Description','Link',
  'How to Apply','Eligibility','Type','Location','Application Window'
];

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Scholarship Import')
    .addItem('Import from Gmail (LLM)', 'importScholarshipsLLM')
    .addItem('Test on 1 newest email', 'debugTestOne')
    .addToUi();
}

function importScholarshipsLLM() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error(`Missing sheet "${SHEET_NAME}"`);
  ensureHeaders_(sheet);
  ensureProcessedSheet_(ss);

  const processed = loadProcessedSet_(ss);
  const threads = GmailApp.search(`label:${LABEL_NAME} newer_than:1y`, 0, 500);

  const rows = [];
  threads.forEach(t => {
    t.getMessages().forEach(msg => {
      const emailId = msg.getId();
      const subject = (msg.getSubject() || '').trim();
      
      if (!subject) return;
      console.log("Subject:", subject);
      // build body (prefer plaintext; fallback to HTML->text)
      let body = (msg.getPlainBody() || '').trim();
      if (body.length < 50) body = htmlToText_(msg.getBody() || '');
      body = stripForwardHeader_(body);

      // Truncate to ~40k chars to control token usage (keeps entire emails in most cases)
      if (body.length > 40000) body = body.slice(0, 40000);

      // LLM extraction
      const items = extractWithLLM_(subject, body);

      // Append rows
      (items || []).forEach(obj => {
        const title = safe(obj.title);
        if (!title) return;
        const key = `${emailId}|${title}`;
        if (processed.has(key)) return;

        rows.push([
          title,
          safe(obj.sponsor),
          safe(obj.amount),
          safe(obj.closing_date || obj.deadline),         // normalized
          safe(obj.description),
          cleanUrl_(safe(obj.link)),
          safe(obj.how_to_apply),
          safe(obj.eligibility),
          safe(obj.type),
          safe(obj.location),
          safe(obj.application_window)
        ]);
        processed.add(key);
      });
    });
  });

  if (rows.length) {
    const sheet = SpreadsheetApp.getActive().getSheetByName(SHEET_NAME);
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, HEADERS.length).setValues(rows);
    saveProcessedSet_(SpreadsheetApp.getActive(), processed);
  }

  SpreadsheetApp.getUi().alert(`Imported ${rows.length} row(s).`);
}

/* ----- Debug: run on just 1 email so you can see output in Logs ----- */
function debugTestOne() {
  const threads = GmailApp.search(`label:${LABEL_NAME}`, 0, 1);
  if (!threads.length) { Logger.log('No threads'); return; }
  const msg = threads[0].getMessages().slice(-1)[0];
  const subject = msg.getSubject();
  let body = (msg.getPlainBody() || '').trim();
  if (body.length < 50) body = htmlToText_(msg.getBody() || '');
  body = stripForwardHeader_(body);
  const items = extractWithLLM_(subject, body);
  Logger.log(JSON.stringify(items, null, 2));
}

/* ================= LLM Extraction ================= */

function extractWithLLM_(subject, body) {
  const schema = {
    type: 'object',
    properties: {
      scholarships: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            title: { type: 'string' },
            sponsor: { type: 'string' },
            amount: { type: 'string' },
            closing_date: { type: 'string' },
            deadline: { type: 'string' },
            description: { type: 'string' },
            link: { type: 'string' },
            how_to_apply: { type: 'string' },
            eligibility: { type: 'string' },
            type: { type: 'string' },
            location: { type: 'string' },
            application_window: { type: 'string' }
          },
          required: ['title']
        }
      }
    },
    required: ['scholarships']
  };

  // Subject-specific hint
  let formatHint = '';
  if (/Scholarship Saturdays/i.test(subject)) {
    formatHint = 'These emails consistently list multiple scholarships in blocks labeled exactly: Title (line), "Sponsor:", "Amount:", "Closing Date:", "Description:", and often a "Website:" link.';
  } else {
    formatHint = 'Fields may appear with labels like "How to Apply:", "Eligibility:", "Type:", "Location:", "Application Window:", and links may appear as "Website:" or bare domains on their own lines.';
  }

  const prompt = [
    {
      role: 'system',
      content:
`You extract scholarship listings from email text and output STRICT JSON that conforms to the provided JSON schema.
Rules:
- Return ONLY JSON, no markdown fences.
- Extract ONE object per scholarship.
- Normalize field names to snake_case in the JSON keys shown below.
- If both "Closing Date" and "Deadline" appear, put the value in "closing_date" and leave "deadline" empty.
- Clean links: remove phrases like "open in new window", trailing dashes or punctuation, and ensure they start with http(s) (add https:// for bare domains).
- Do not invent data; if a field is missing, use an empty string.`
    },
    {
      role: 'user',
      content:
`Subject: ${subject}

Format hint: ${formatHint}

Email body:
<<<
${body}
>>>

Return JSON matching this TypeScript shape:
{
  "scholarships": [
    {
      "title": string,
      "sponsor": string,
      "amount": string,
      "closing_date": string,
      "deadline": string,
      "description": string,
      "link": string,
      "how_to_apply": string,
      "eligibility": string,
      "type": string,
      "location": string,
      "application_window": string
    }, ...
  ]
}`
    }
  ];

  try {
    let jsonText = '';
    if (PROVIDER === 'openai') {
      jsonText = callOpenAI_(prompt, schema);
    } else if (PROVIDER === 'anthropic') {
      jsonText = callAnthropic_(prompt, schema);
    } else if (PROVIDER === 'gemini') {
      jsonText = callGemini_(prompt, schema);
    } else {
      throw new Error('Unsupported PROVIDER.');
    }

    // Some models sometimes wrap in code fences—strip if present
    jsonText = jsonText.trim().replace(/^```json\s*/i, '').replace(/```$/i, '').trim();
    const parsed = JSON.parse(jsonText);
    const items = (parsed && parsed.scholarships) ? parsed.scholarships : [];
    // final link clean + normalization
    items.forEach(it => {
      if (!it) return;
      it.link = cleanUrl_(safe(it.link));
      if (!safe(it.closing_date) && safe(it.deadline)) {
        it.closing_date = it.deadline; it.deadline = '';
      }
    });
    return items;
  } catch (e) {
    Logger.log('LLM parse error: ' + e);
    return [];
  }
}

/* ================ Provider Calls ================== */

function callOpenAI_(messages, jsonSchema) {
  const key = PropertiesService.getScriptProperties().getProperty('OPENAI_API_KEY');
  if (!key) throw new Error('Set OPENAI_API_KEY in Script properties.');
  const url = 'https://api.openai.com/v1/chat/completions';
  const payload = {
    model: MODEL_OPENAI,
    messages: messages,
    temperature: 0,
    response_format: { type: 'json_object' } // forces JSON
  };
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: `Bearer ${key}` },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const data = JSON.parse(res.getContentText());
  if (!data.choices || !data.choices[0]) throw new Error('OpenAI response error: ' + res.getContentText());
  return data.choices[0].message.content;
}

function callAnthropic_(messages, jsonSchema) {
  const key = PropertiesService.getScriptProperties().getProperty('ANTHROPIC_API_KEY');
  if (!key) throw new Error('Set ANTHROPIC_API_KEY in Script properties.');
  const url = 'https://api.anthropic.com/v1/messages';
  // Convert OpenAI-style to Anthropic
  const sys = (messages.find(m => m.role === 'system') || {}).content || '';
  const user = (messages.find(m => m.role === 'user') || {}).content || '';
  const payload = {
    model: MODEL_ANTHROPIC,
    max_tokens: 2000,
    temperature: 0,
    system: sys,
    messages: [{ role: 'user', content: user }]
  };
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'x-api-key': key,
      'anthropic-version': '2023-06-01'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const data = JSON.parse(res.getContentText());
  if (!data.content || !data.content[0] || !data.content[0].text) throw new Error('Anthropic response error: ' + res.getContentText());
  return data.content[0].text;
}

function callGemini_(messages, jsonSchema) {
  const key = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!key) throw new Error('Set GEMINI_API_KEY in Script properties.');
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL_GEMINI}:generateContent?key=${encodeURIComponent(key)}`;

  const sys = (messages.find(m => m.role === 'system') || {}).content || '';
  const user = (messages.find(m => m.role === 'user') || {}).content || '';

  const payload = {
    contents: [{ role: 'user', parts: [{ text: sys + '\n\n' + user }]}],
    generationConfig: { temperature: 0 }
  };
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const data = JSON.parse(res.getContentText());
  const text = (data.candidates && data.candidates[0] && data.candidates[0].content && data.candidates[0].content.parts && data.candidates[0].content.parts[0].text) || '';
  if (!text) throw new Error('Gemini response error: ' + res.getContentText());
  return text;
}

/* ================= Utilities ===================== */

function ensureHeaders_(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow === 0) {
    sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    return;
  }
  const have = sheet.getRange(1, 1, 1, HEADERS.length).getValues()[0];
  const need = HEADERS.some((h, i) => (have[i] || '').toString().trim() !== h);
  if (need) sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
}

function ensureProcessedSheet_(ss) {
  let p = ss.getSheetByName(PROCESSED_SHEET);
  if (!p) {
    p = ss.insertSheet(PROCESSED_SHEET);
    p.hideSheet();
    p.getRange(1, 1).setValue('emailId|title');
  }
}
function loadProcessedSet_(ss) {
  const p = ss.getSheetByName(PROCESSED_SHEET);
  const last = p.getLastRow();
  const keys = last > 1 ? p.getRange(2, 1, last - 1, 1).getValues().flat() : [];
  return new Set(keys);
}
function saveProcessedSet_(ss, setObj) {
  const p = ss.getSheetByName(PROCESSED_SHEET);
  p.clearContents();
  p.getRange(1, 1).setValue('emailId|title');
  const arr = Array.from(setObj).map(k => [k]);
  if (arr.length) p.getRange(2, 1, arr.length, 1).setValues(arr);
}

function htmlToText_(html) {
  return (html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|li|h\d)>/gi, '\n')
    .replace(/<li>/gi, '• ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/<[^>]+>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function stripForwardHeader_(txt) {
  const i = txt.search(/Begin forwarded message/i);
  if (i >= 0) {
    const cut = txt.slice(i);
    return cut.replace(/^.*?Subject:\s*[^\n]*\n/i, '');
  }
  return txt;
}

function cleanUrl_(u) {
  let x = (u || '').trim();
  if (!x) return '';
  x = x.replace(/\s*[-–—]?\s*open in new window.*$/i, '');
  x = x.replace(/^<|>$/g, '');
  // add protocol for bare domains
  if (!/^https?:\/\//i.test(x) && /\b[a-z0-9-]+\.[a-z]{2,}\b/i.test(x)) x = 'https://' + x;
  // strip trailing punctuation/dashes
  while (/[)\].,;:!?'\"»›…\-–—]$/.test(x)) x = x.slice(0, -1);
  return x;
}

function safe(v) { return (v == null) ? '' : String(v).trim(); }
