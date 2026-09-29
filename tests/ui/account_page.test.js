// Runs the account page's own scripts in jsdom (a simulated browser) and clicks through the
// manual-match panel and the record table. Called by suite_ui.py with the rendered page's path.
const fs = require("fs");
const { JSDOM, VirtualConsole } = require("jsdom");
const html = fs.readFileSync(process.argv[2], "utf8");
const errors = [];
const vc = new VirtualConsole();
vc.on("jsdomError", e => errors.push(String(e.message || e)));
vc.on("error", e => errors.push(String(e)));
const dom = new JSDOM(html, { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const { window } = dom; const d = window.document;
window.HTMLElement.prototype.scrollIntoView = function () {};
let confirmMsg = null; window.confirm = m => { confirmMsg = m; return false; };

let fails = 0;
const check = (label, cond) => { console.log((cond ? "PASS " : "FAIL ") + label); if (!cond) fails++; };
const row = (list, text) => [...d.querySelectorAll(`#${list} .mmrow`)].find(r => r.textContent.includes(text));
const tick = (list, text, on = true) => { const cb = row(list, text).querySelector("input"); cb.checked = on;
  cb.dispatchEvent(new window.Event("change", { bubbles: true })); };
const order = () => [...d.querySelectorAll("#mmb .mmrow .mmw")].map(e => e.textContent.trim().split(" ")[0] + " " + e.textContent.trim().split(" ")[1]);
const sum = () => d.getElementById("mmsum").textContent;
const go = () => d.getElementById("mmgo");

check("no script errors on load", errors.length === 0);
check("record-table account pickers filled from the chart of accounts",
      [...d.querySelectorAll("select.acct")].every(s => s.options.length >= 3));
check("transfer group offered last in every picker, with your other bank",
      [...d.querySelectorAll("select.acct")].every(s => { const g = [...s.querySelectorAll("optgroup")].pop();
        return g && /^Transfer (to|from) your account$/.test(g.label) && g.textContent.includes("Centenary"); }));
check("button starts disabled", go().disabled && /Tick at least one/.test(sum()));

tick("mml", "DEPOSIT CASH");
console.log("   order after ticking 300,000 deposit:", order().slice(0, 3).join(" | "));
check("ticking a bank line puts the closest amount first", order()[0].startsWith("Deposit"));
check("still disabled with one side only", go().disabled);
tick("mmb", "Cust B"); tick("mmb", "Cust A");
console.log("   bar:", sum());
check("running totals + difference", /Bank 300,000\.00/.test(sum()) && /QuickBooks 299,000\.00/.test(sum()) && /Difference 1,000\.00/.test(sum()));
check("difference shown as a warning, button enabled", d.getElementById("mmsum").className === "warn" && !go().disabled);
check("ticked rows highlighted", row("mmb", "Cust A").classList.contains("on"));

const f = d.getElementById("mmform");
const ev = new window.Event("submit", { cancelable: true, bubbles: true });
f.dispatchEvent(ev);
check("submitting with a difference asks first (and cancel stops it)", /differ by 1,000\.00/.test(confirmMsg || "") && ev.defaultPrevented);

tick("mmb", "Cust A", false); tick("mmb", "Cust B", false); tick("mmb", "Deposit");
check("exact pair shows zero difference in green", /Difference 0\.00/.test(sum()) && d.getElementById("mmsum").className === "ok");

const s = d.querySelector('.mmsearch[data-list=mmb]'); s.value = "airtel"; s.dispatchEvent(new window.Event("input"));
const visible = [...d.querySelectorAll("#mmb .mmrow")].filter(r => r.style.display !== "none");
check("search filters the QuickBooks list", visible.length === 1 && visible[0].textContent.includes("Airtel"));
s.value = ""; s.dispatchEvent(new window.Event("input"));

const btn = d.querySelector(".mm-open");
check("duplicate warning has a 'Match it instead' button", !!btn);
btn.click();
const ml = [...d.querySelectorAll("#mml input:checked")].map(i => i.closest(".mmrow").textContent);
const mb = [...d.querySelectorAll("#mmb input:checked")].map(i => i.closest(".mmrow").textContent);
check("'Match it instead' preselects exactly that pair", ml.length === 1 && ml[0].includes("SUPPLIER Y") && mb.length === 1 && mb[0].includes("Supplier Y"));
check("…and it balances", /Difference 0\.00/.test(sum()));

confirmMsg = null;
const rf = d.getElementById("recform");
const boxes = [...rf.querySelectorAll(".rsel")];
check("nothing pre-ticked without a suggested account", boxes.every(b => !b.checked));
const pick = t => boxes.find(b => b.closest("tr").textContent.includes(t));
pick("NEW EXPENSE").checked = true; pick("TRANSFER TO SAVINGS").checked = true;
const bulk = rf.querySelector("button[name=bulk]");
const e2 = new window.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: bulk });
rf.dispatchEvent(e2);
console.log("   bulk confirm:", confirmMsg);
check("bulk record confirms count and total (flagged lines excluded)", /^Record 2 transactions totalling 510,000\.00 in QuickBooks\?$/.test(confirmMsg || ""));

check("no script errors during interaction", errors.length === 0);
if (errors.length) console.log(errors);
console.log(`\n${fails} failure(s)`); process.exit(fails ? 1 : 0);
