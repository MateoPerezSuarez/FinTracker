"""
Seq2Seq Summarization with Attention
Generates short headlines (10-20 tokens) from news articles
Compares FastText (non-contextual) vs BERT (contextual) embeddings
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import pandas as pd
import numpy as np
from gensim.models import KeyedVectors
from transformers import BertModel, BertTokenizer
from sklearn.model_selection import train_test_split
import random
from tqdm import tqdm
import os

# Set random seeds for reproducibility
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

# Device configuration
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Hyperparameters
MAX_HEADLINE_LEN = 20  # Maximum headline length (10-20 tokens)
MAX_ARTICLE_LEN = 512  # Maximum article length
HIDDEN_SIZE = 256
EMBEDDING_DIM = 300  # For FastText
BATCH_SIZE = 32
LEARNING_RATE = 0.001
NUM_EPOCHS = 20
TEACHER_FORCING_RATIO = 0.5
PATIENCE = 3  # For early stopping

# Special tokens
PAD_TOKEN = '<PAD>'
SOS_TOKEN = '<SOS>'
EOS_TOKEN = '<EOS>'
UNK_TOKEN = '<UNK>'


class Vocabulary:
    """Build vocabulary from text"""
    def __init__(self):
        self.word2idx = {PAD_TOKEN: 0, SOS_TOKEN: 1, EOS_TOKEN: 2, UNK_TOKEN: 3}
        self.idx2word = {0: PAD_TOKEN, 1: SOS_TOKEN, 2: EOS_TOKEN, 3: UNK_TOKEN}
        self.word_count = {}
        self.n_words = 4
    
    def add_sentence(self, sentence):
        for word in sentence.split():
            self.add_word(word)
    
    def add_word(self, word):
        if word not in self.word2idx:
            self.word2idx[word] = self.n_words
            self.idx2word[self.n_words] = word
            self.word_count[word] = 1
            self.n_words += 1
        else:
            self.word_count[word] += 1


class SummarizationDataset(Dataset):
    """Dataset for summarization task"""
    def __init__(self, articles, headlines, vocab, max_article_len, max_headline_len):
        self.articles = articles
        self.headlines = headlines
        self.vocab = vocab
        self.max_article_len = max_article_len
        self.max_headline_len = max_headline_len
    
    def __len__(self):
        return len(self.articles)
    
    def __getitem__(self, idx):
        article = self.articles[idx]
        headline = self.headlines[idx]
        
        # Convert to indices
        article_indices = [self.vocab.word2idx.get(word, self.vocab.word2idx[UNK_TOKEN]) 
                          for word in article.split()[:self.max_article_len]]
        headline_indices = [self.vocab.word2idx[SOS_TOKEN]] + \
                          [self.vocab.word2idx.get(word, self.vocab.word2idx[UNK_TOKEN]) 
                           for word in headline.split()[:self.max_headline_len-1]] + \
                          [self.vocab.word2idx[EOS_TOKEN]]
        
        # Pad sequences
        article_len = len(article_indices)
        headline_len = len(headline_indices)
        
        article_indices += [self.vocab.word2idx[PAD_TOKEN]] * (self.max_article_len - article_len)
        headline_indices += [self.vocab.word2idx[PAD_TOKEN]] * (self.max_headline_len + 1 - headline_len)
        
        return {
            'article': torch.tensor(article_indices, dtype=torch.long),
            'headline': torch.tensor(headline_indices, dtype=torch.long),
            'article_len': article_len,
            'headline_len': headline_len
        }


class EncoderRNN(nn.Module):
    """Bidirectional GRU Encoder"""
    def __init__(self, vocab_size, embedding_dim, hidden_size, embedding_weights=None, freeze_embeddings=False):
        super(EncoderRNN, self).__init__()
        self.hidden_size = hidden_size
        
        if embedding_weights is not None:
            self.embedding = nn.Embedding.from_pretrained(embedding_weights, freeze=freeze_embeddings, padding_idx=0)
        else:
            self.embedding = nn.Embedding(vocab_size, embedding_dim, padding_idx=0)
        
        self.gru = nn.GRU(embedding_dim, hidden_size, batch_first=True, bidirectional=True)
    
    def forward(self, input_seq, input_lengths):
        embedded = self.embedding(input_seq)
        
        # Pack padded sequences
        packed = nn.utils.rnn.pack_padded_sequence(embedded, input_lengths.cpu(), 
                                                    batch_first=True, enforce_sorted=False)
        outputs, hidden = self.gru(packed)
        
        # Unpack sequences
        outputs, _ = nn.utils.rnn.pad_packed_sequence(outputs, batch_first=True)
        
        # Sum bidirectional outputs
        outputs = outputs[:, :, :self.hidden_size] + outputs[:, :, self.hidden_size:]
        
        # Sum bidirectional hidden states
        hidden = hidden[0] + hidden[1]
        hidden = hidden.unsqueeze(0)
        
        return outputs, hidden


class BahdanauAttention(nn.Module):
    """Bahdanau Attention Mechanism"""
    def __init__(self, hidden_size):
        super(BahdanauAttention, self).__init__()
        self.Wa = nn.Linear(hidden_size, hidden_size)
        self.Ua = nn.Linear(hidden_size, hidden_size)
        self.Va = nn.Linear(hidden_size, 1)
    
    def forward(self, query, keys):
        
        scores = self.Va(torch.tanh(self.Wa(query.unsqueeze(1)) + self.Ua(keys)))
        scores = scores.squeeze(2)
        
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), keys)
        context = context.squeeze(1)
        
        return context, weights


class AttnDecoderRNN(nn.Module):
    """GRU Decoder with Attention"""
    def __init__(self, vocab_size, embedding_dim, hidden_size, embedding_weights=None, freeze_embeddings=False):
        super(AttnDecoderRNN, self).__init__()
        self.hidden_size = hidden_size
        self.vocab_size = vocab_size
        
        if embedding_weights is not None:
            self.embedding = nn.Embedding.from_pretrained(embedding_weights, freeze=freeze_embeddings, padding_idx=0)
        else:
            self.embedding = nn.Embedding(vocab_size, embedding_dim, padding_idx=0)
        
        self.attention = BahdanauAttention(hidden_size)
        self.gru = nn.GRU(embedding_dim + hidden_size, hidden_size, batch_first=True)
        self.out = nn.Linear(hidden_size, vocab_size)
    
    def forward(self, input_token, hidden, encoder_outputs):
        embedded = self.embedding(input_token).unsqueeze(1)  # [batch_size, 1, embedding_dim]
        
        # Attention
        context, attn_weights = self.attention(hidden.squeeze(0), encoder_outputs)
        
        # Concatenate embedded input and context
        rnn_input = torch.cat([embedded, context.unsqueeze(1)], dim=2)
        
        output, hidden = self.gru(rnn_input, hidden)
        output = self.out(output.squeeze(1))
        
        return output, hidden, attn_weights


class Seq2Seq(nn.Module):
    def __init__(self, encoder, decoder):
        super(Seq2Seq, self).__init__()
        self.encoder = encoder
        self.decoder = decoder
    
    def forward(self, src, src_len, trg, teacher_forcing_ratio=0.5):
        batch_size = src.size(0)
        trg_len = trg.size(1)
        trg_vocab_size = self.decoder.vocab_size
        
        outputs = torch.zeros(batch_size, trg_len, trg_vocab_size).to(src.device)
        
        encoder_outputs, hidden = self.encoder(src, src_len)
        
        # First input to decoder is SOS token
        input_token = trg[:, 0]
        
        for t in range(1, trg_len):
            output, hidden, _ = self.decoder(input_token, hidden, encoder_outputs)
            outputs[:, t] = output
            
            # Teacher forcing
            teacher_force = random.random() < teacher_forcing_ratio
            top1 = output.argmax(1)
            input_token = trg[:, t] if teacher_force else top1
        
        return outputs


def load_fasttext_embeddings(vocab, model_path):
    """Load FastText embeddings"""
    print("Loading FastText model...")
    model = KeyedVectors.load(model_path)
    
    embedding_matrix = np.zeros((vocab.n_words, EMBEDDING_DIM))
    
    for word, idx in vocab.word2idx.items():
        if word in model:
            embedding_matrix[idx] = model[word]
        else:
            embedding_matrix[idx] = np.random.normal(0, 0.1, EMBEDDING_DIM)
    
    return torch.FloatTensor(embedding_matrix)


def train_epoch(model, dataloader, optimizer, criterion, teacher_forcing_ratio):
    """Train for one epoch"""
    model.train()
    epoch_loss = 0
    
    for batch in tqdm(dataloader, desc="Training"):
        article = batch['article'].to(device)
        headline = batch['headline'].to(device)
        article_len = batch['article_len']
        
        optimizer.zero_grad()
        
        output = model(article, article_len, headline, teacher_forcing_ratio)
        
        # Reshape for loss calculation
        output_dim = output.shape[-1]
        output = output[:, 1:].reshape(-1, output_dim)
        headline = headline[:, 1:].reshape(-1)
        
        loss = criterion(output, headline)
        loss.backward()
        
        # Gradient clipping
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        
        optimizer.step()
        epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def evaluate(model, dataloader, criterion):
    """Evaluate the model"""
    model.eval()
    epoch_loss = 0
    
    with torch.no_grad():
        for batch in dataloader:
            article = batch['article'].to(device)
            headline = batch['headline'].to(device)
            article_len = batch['article_len']
            
            output = model(article, article_len, headline, 0)  # No teacher forcing
            
            output_dim = output.shape[-1]
            output = output[:, 1:].reshape(-1, output_dim)
            headline = headline[:, 1:].reshape(-1)
            
            loss = criterion(output, headline)
            epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def generate_headline(model, article, vocab, max_len=MAX_HEADLINE_LEN):
    """Generate headline for a single article"""
    model.eval()
    
    with torch.no_grad():
        # Tokenize and convert to indices
        article_indices = [vocab.word2idx.get(word, vocab.word2idx[UNK_TOKEN]) 
                          for word in article.split()[:MAX_ARTICLE_LEN]]
        article_len = len(article_indices)
        article_indices += [vocab.word2idx[PAD_TOKEN]] * (MAX_ARTICLE_LEN - article_len)
        
        article_tensor = torch.tensor([article_indices], dtype=torch.long).to(device)
        article_len_tensor = torch.tensor([article_len], dtype=torch.long)
        
        encoder_outputs, hidden = model.encoder(article_tensor, article_len_tensor)
        
        # Start with SOS token
        input_token = torch.tensor([vocab.word2idx[SOS_TOKEN]], dtype=torch.long).to(device)
        
        generated = []
        for _ in range(max_len):
            output, hidden, _ = model.decoder(input_token, hidden, encoder_outputs)
            top1 = output.argmax(1)
            
            predicted_word = vocab.idx2word[top1.item()]
            
            if predicted_word == EOS_TOKEN:
                break
            
            generated.append(predicted_word)
            input_token = top1
        
        return ' '.join(generated)


def calculate_rouge_n(reference, hypothesis, n=1):
    """Simple n-gram overlap calculation (ROUGE-N style)"""
    def get_ngrams(text, n):
        words = text.split()
        return set([' '.join(words[i:i+n]) for i in range(len(words)-n+1)])
    
    ref_ngrams = get_ngrams(reference, n)
    hyp_ngrams = get_ngrams(hypothesis, n)
    
    if len(ref_ngrams) == 0:
        return 0.0
    
    overlap = len(ref_ngrams & hyp_ngrams)
    return overlap / len(ref_ngrams)


def main():
    """Main training and evaluation function"""
    
    # Load data
    print("Loading data...")
    df = pd.read_csv('data/definitivos/INDEX_ALL_scrapped_filtrado.csv')
    
    # Clean and prepare data
    df = df[['article_text', 'headline']].dropna()
    df['article_text'] = df['article_text'].str.lower().str.strip()
    df['headline'] = df['headline'].str.lower().str.strip()
    
    # Filter headlines to reasonable length
    df = df[df['headline'].str.split().str.len() <= MAX_HEADLINE_LEN]
    df = df[df['headline'].str.split().str.len() >= 3]
    
    print(f"Total samples: {len(df)}")
    
    # Split data
    train_df, temp_df = train_test_split(df, test_size=0.3, random_state=SEED)
    val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=SEED)
    
    print(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")
    
    # Build vocabulary
    print("Building vocabulary...")
    vocab = Vocabulary()
    for _, row in train_df.iterrows():
        vocab.add_sentence(row['article_text'])
        vocab.add_sentence(row['headline'])
    
    print(f"Vocabulary size: {vocab.n_words}")
    
    # Create datasets
    train_dataset = SummarizationDataset(
        train_df['article_text'].tolist(),
        train_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    val_dataset = SummarizationDataset(
        val_df['article_text'].tolist(),
        val_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    test_dataset = SummarizationDataset(
        test_df['article_text'].tolist(),
        test_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)
    
    # Load FastText embeddings
    fasttext_path = 'src/Representacion_del_lenguaje/embeddings/NoContext/fasttext_sg.kv'
    if os.path.exists(fasttext_path):
        print("Loading FastText embeddings...")
        embedding_weights = load_fasttext_embeddings(vocab, fasttext_path)
    else:
        print("FastText model not found, using random embeddings")
        embedding_weights = None
    
    # Create model
    print("Creating model...")
    encoder = EncoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, embedding_weights).to(device)
    decoder = AttnDecoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, embedding_weights).to(device)
    model = Seq2Seq(encoder, decoder).to(device)
    
    # Loss and optimizer
    criterion = nn.CrossEntropyLoss(ignore_index=vocab.word2idx[PAD_TOKEN])
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    
    # Training loop with early stopping
    print("\nStarting training...")
    best_val_loss = float('inf')
    patience_counter = 0
    
    for epoch in range(NUM_EPOCHS):
        train_loss = train_epoch(model, train_loader, optimizer, criterion, TEACHER_FORCING_RATIO)
        val_loss = evaluate(model, val_loader, criterion)
        
        print(f'Epoch {epoch+1}/{NUM_EPOCHS}')
        print(f'  Train Loss: {train_loss:.4f}')
        print(f'  Val Loss: {val_loss:.4f}')
        
        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), 'best_model.pt')
            print('  Model saved!')
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                print(f'Early stopping at epoch {epoch+1}')
                break
    
    # Load best model
    model.load_state_dict(torch.load('best_model.pt'))
    
    # Evaluate on test set
    print("\nEvaluating on test set...")
    test_loss = evaluate(model, test_loader, criterion)
    print(f'Test Loss: {test_loss:.4f}')
    
    # Generate sample headlines
    print("\n" + "="*80)
    print("SAMPLE GENERATIONS")
    print("="*80)
    
    test_samples = test_df.sample(10, random_state=SEED)
    rouge1_scores = []
    rouge2_scores = []
    
    for idx, row in test_samples.iterrows():
        article = row['article_text']
        reference = row['headline']
        generated = generate_headline(model, article, vocab)
        
        # Calculate ROUGE scores
        r1 = calculate_rouge_n(reference, generated, 1)
        r2 = calculate_rouge_n(reference, generated, 2)
        rouge1_scores.append(r1)
        rouge2_scores.append(r2)
        
        print(f"\nArticle: {article[:200]}...")
        print(f"Reference: {reference}")
        print(f"Generated: {generated}")
        print(f"ROUGE-1: {r1:.3f}, ROUGE-2: {r2:.3f}")
    
    print("\n" + "="*80)
    print(f"Average ROUGE-1: {np.mean(rouge1_scores):.3f}")
    print(f"Average ROUGE-2: {np.mean(rouge2_scores):.3f}")
    print("="*80)


if __name__ == "__main__":
    main()
