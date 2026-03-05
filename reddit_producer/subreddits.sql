

DELETE FROM subreddit_config;

INSERT INTO subreddit_config (name, interval_seconds, priority, is_active) VALUES

-- ══════════════════════════════════════════════════════════════════
-- FAST TIER (60s) — Reddit's highest volume subs
-- 30M–67M subscribers, hundreds of posts per hour
-- These dominate Reddit's front page 24/7
-- ══════════════════════════════════════════════════════════════════

-- Humor & Viral (highest raw post volume on all of Reddit)
('funny',                  60, 'fast', true),
('memes',                  60, 'fast', true),
('dankmemes',              60, 'fast', true),
('me_irl',                 60, 'fast', true),
('cursedcomments',         60, 'fast', true),
('nextfuckinglevel',       60, 'fast', true),
('interestingasfuck',      60, 'fast', true),
('MadeMeSmile',            60, 'fast', true),
('oddlysatisfying',        60, 'fast', true),

-- Q&A & Discussion (massive comment volumes, great NLP signal)
('AskReddit',              60, 'fast', true),
('OutOfTheLoop',           60, 'fast', true),
('NoStupidQuestions',      60, 'fast', true),
('explainlikeimfive',      60, 'fast', true),
('Showerthoughts',         60, 'fast', true),
('changemyview',           60, 'fast', true),
('AmItheAsshole',          60, 'fast', true),
('relationship_advice',    60, 'fast', true),

-- News & World Events (breaks first on Reddit before mainstream media)
('worldnews',              60, 'fast', true),
('news',                   60, 'fast', true),
('politics',               60, 'fast', true),
('UpliftingNews',          60, 'fast', true),
('nottheonion',            60, 'fast', true),
('todayilearned',          60, 'fast', true),

-- Gaming (17% of Reddit traffic, second only to adult content)
('gaming',                 60, 'fast', true),
('pcgaming',               60, 'fast', true),
('GameDeals',              60, 'fast', true),

-- ══════════════════════════════════════════════════════════════════
-- MEDIUM TIER (180s) — High engagement, focused communities
-- 5M–30M subscribers, strong comment-to-post ratios
-- ══════════════════════════════════════════════════════════════════

-- AI / Tech (quadrupled in volume 2024-2025 per data)
('technology',            180, 'medium', true),
('artificial',            180, 'medium', true),
('ChatGPT',               180, 'medium', true),
('LocalLLaMA',            180, 'medium', true),
('StableDiffusion',       180, 'medium', true),
('OpenAI',                180, 'medium', true),
('singularity',           180, 'medium', true),
('MachineLearning',       180, 'medium', true),
('programming',           180, 'medium', true),
('learnprogramming',      180, 'medium', true),
('Python',                180, 'medium', true),
('javascript',            180, 'medium', true),
('webdev',                180, 'medium', true),
('linux',                 180, 'medium', true),
('cybersecurity',         180, 'medium', true),

-- Finance (surged Q1 2025 during volatile S&P — very high engagement)
('wallstreetbets',        180, 'medium', true),
('stocks',                180, 'medium', true),
('investing',             180, 'medium', true),
('personalfinance',       180, 'medium', true),
('CryptoCurrency',        180, 'medium', true),
('Bitcoin',               180, 'medium', true),
('Superstonk',            180, 'medium', true),
('financialindependence', 180, 'medium', true),

-- Sports (event-driven spikes, massive real-time engagement)
('nba',                   180, 'medium', true),
('nfl',                   180, 'medium', true),
('soccer',                180, 'medium', true),
('formula1',              180, 'medium', true),
('baseball',              180, 'medium', true),
('hockey',                180, 'medium', true),
('tennis',                180, 'medium', true),

-- Entertainment & Pop Culture
('movies',                180, 'medium', true),
('television',            180, 'medium', true),
('anime',                 180, 'medium', true),
('marvelstudios',         180, 'medium', true),
('StarWars',              180, 'medium', true),
('music',                 180, 'medium', true),
('hiphopheads',           180, 'medium', true),
('books',                 180, 'medium', true),

-- Lifestyle (growing fast in 2025 per data)
('AskMen',                180, 'medium', true),
('AskWomen',              180, 'medium', true),
('loseit',                180, 'medium', true),
('fitness',               180, 'medium', true),
('DIY',                   180, 'medium', true),
('cooking',               180, 'medium', true),
('food',                  180, 'medium', true),

-- ══════════════════════════════════════════════════════════════════
-- SLOW TIER (600s) — Niche but deeply engaged
-- High comment-to-upvote ratios = richest NLP signal
-- Users spending 2x site average time per post
-- ══════════════════════════════════════════════════════════════════

-- Deep Tech
('rust',                  600, 'slow', true),
('golang',                600, 'slow', true),
('devops',                600, 'slow', true),
('aws',                   600, 'slow', true),
('kubernetes',            600, 'slow', true),
('selfhosted',            600, 'slow', true),
('homelab',               600, 'slow', true),
('netsec',                600, 'slow', true),
('ReverseEngineering',    600, 'slow', true),
('datascience',           600, 'slow', true),
('compsci',               600, 'slow', true),

-- Science & Academia
('science',               600, 'slow', true),
('physics',               600, 'slow', true),
('math',                  600, 'slow', true),
('neuroscience',          600, 'slow', true),
('chemistry',             600, 'slow', true),
('space',                 600, 'slow', true),
('Astronomy',             600, 'slow', true),
('biology',               600, 'slow', true),
('medicine',              600, 'slow', true),
('AskScience',            600, 'slow', true),
('Futurology',            600, 'slow', true),

-- Finance Deep Cuts (highest comment quality on Reddit)
('algotrading',           600, 'slow', true),
('SecurityAnalysis',      600, 'slow', true),
('ValueInvesting',        600, 'slow', true),
('Bogleheads',            600, 'slow', true),
('quant',                 600, 'slow', true),
('economics',             600, 'slow', true),

-- Ideas / Society (very high comment-to-post ratios)
('geopolitics',           600, 'slow', true),
('history',               600, 'slow', true),
('philosophy',            600, 'slow', true),
('TrueReddit',            600, 'slow', true),
('slatestarcodex',        600, 'slow', true),

('law',                   600, 'slow', true),

-- Hobbies with 2x avg time-on-page (per Reddit internal data)
('boardgames',            600, 'slow', true),
('photography',           600, 'slow', true),
('woodworking',           600, 'slow', true),
('3Dprinting',            600, 'slow', true),
('MechanicalKeyboards',   600, 'slow', true),
('bicycling',             600, 'slow', true),
('running',               600, 'slow', true),
('climbing',              600, 'slow', true),
('solotravel',            600, 'slow', true),
('ExperiencedDevs',       600, 'slow', true);
