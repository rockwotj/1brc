#include "seastar/core/seastar.hh"
#include "seastar/core/smp.hh"
#include "thirdparty/fast_float.h"
#include "thirdparty/stringzilla.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/stream.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>

#include <exception>
#include <limits>
#include <string_view>

static seastar::logger lg("1brc-log");

namespace ss {
using namespace seastar;
}

#ifdef NDEBUG
#define DCHECK(x)
#else
#define DCHECK(x) assert(x)
#endif

using ss::operator""_MiB;

struct raw_measurement {
    ss::sstring location;
    double reading;
};

struct parse_result {
    std::string_view::iterator pos;
    bool more;
};

// Using some super fast libraries, parse out the `<location>;<reading>\n`
// format of the file.
parse_result parse_measurements_fast(
  std::string_view s, ss::noncopyable_function<void(raw_measurement)> cb) {
    std::string_view::iterator it = s.begin();
    while (it != s.end()) {
        const char* pos = sz_find_1char(it, s.end() - it, ";");
        if (pos == nullptr) {
            return parse_result{.pos = it, .more = true};
        }
        double number;
        auto result = fast_float::from_chars(
          pos + 1, s.end(), number, fast_float::chars_format::fixed);
        // We need the newline to complete the reading, if we got a bad number,
        // assume we're missing data.
        if (result.ec != std::errc() || result.ptr == s.end()) {
            return parse_result{.pos = it, .more = true};
        }
        DCHECK(*result.ptr == '\n');
        cb({.location = {it, pos}, .reading = number});
        it = result.ptr + 1;
    }
    // Amazing this buffer ended with a newline
    return parse_result{.pos = it, .more = false};
}

class measurement_aggregator {
public:
    void add_reading(double reading) {
        _min = std::min(_min, reading);
        _max = std::max(_max, reading);
        _sum += reading;
        _count += 1;
    }

    void merge(const measurement_aggregator& other) {
        _min = std::min(_min, other._min);
        _max = std::max(_max, other._max);
        _sum += other._sum;
        _count += other._count;
    }

    friend std::ostream&
    operator<<(std::ostream& os, const measurement_aggregator& agg) {
        return os << agg._min << "/" << agg.mean() << "/" << agg._max;
    }

private:
    double mean() const {
        return _sum / _count;
    }

    double _min = 0.0, _max = 0.0, _sum = 0.0;
    size_t _count = 0;
};

using results_map = absl::btree_map<ss::sstring, measurement_aggregator>;

void print_results(const results_map& results) {
    std::cout << "{";
    if (!results.empty()) {
        auto it = results.begin();
        std::cout << it->first << "=" << it->second;
        while (++it != results.end()) {
            std::cout << "," << it->first << "=" << it->second;
        }
    }
    std::cout << "}\n";
}

struct file_split {
    size_t offset;
    size_t len;
};

class parser {
public:
    ss::future<> start(ss::sstring filename, file_split split) {
        _split = split;
        _stream = ss::make_file_input_stream(
          co_await ss::open_file_dma(filename, ss::open_flags::ro),
          /*offset=*/split.offset,
          /*len=*/std::numeric_limits<size_t>::max(),
          {
            .buffer_size = 2_MiB,
            .read_ahead = 16,
          });
    }

    ss::future<> stop() {
        co_await _stream.close();
    }

    results_map take_results() {
        return std::move(_results);
    }

    ss::future<> run() {
        ss::sstring leftovers;
        // Since we don't know the bounds in advance of each row (in terms of
        // byte size, as each entry is of variable length), we start at some
        // offset and skip the first one, then we go until we've **parsed** the
        // length required, so we may overread (but the next shard should be
        // skipping the partial read).
        size_t len_parsed = 0;
        bool ready = _split.len == 0;
        while (!_stream.eof() && len_parsed < _split.len) {
            auto buf = co_await _stream.read();
            if (buf.empty()) {
                continue;
            }
            if (!ready) {
                auto skipped = read_line(buf); // Skip the first partial line
                len_parsed += skipped.size();
                ready = true;
            }
            // When the previous line had some data leftover due to a partial
            // line, we need to process it.
            if (!leftovers.empty()) {
                // Otherwise find the rest of the line
                std::string_view prev_line_extra = read_line(buf);
                leftovers.append(
                  prev_line_extra.data(), prev_line_extra.size());
                auto result = parse_measurements_fast(
                  leftovers, [this](raw_measurement raw) {
                      _results[raw.location].add_reading(raw.reading);
                  });
                DCHECK(!result.more);
                DCHECK(result.pos == leftovers.end());
                len_parsed += leftovers.size();
            }
            auto result = parse_measurements_fast(
              {buf.get(), buf.size()}, [this](raw_measurement raw) {
                  _results[raw.location].add_reading(raw.reading);
              });
            len_parsed += result.pos - buf.get();
            leftovers = result.more ? ss::sstring(result.pos, buf.end())
                                    : ss::sstring();
        }
        DCHECK(leftovers.empty());
    }

private:
    std::string_view read_line(ss::temporary_buffer<char>& buf) {
        const char* start = buf.get();
        const char* line_end = sz_find_1char(start, buf.size(), "\n");
        // ASSUMPTION: is that a line is never longer than a DMA read
        // buffer
        DCHECK(line_end != nullptr);
        size_t prev_len = (line_end - start) + 1;
        buf.trim_front(prev_len);
        // This might look scary, but trim doesn't free the underlying memory
        return {start, prev_len};
    }

    file_split _split;
    ss::input_stream<char> _stream;
    results_map _results;
};

ss::future<results_map> run_one(ss::sstring input_file, file_split split) {
    parser p;
    co_await p.start(input_file, split);
    auto fut = co_await ss::coroutine::as_future(p.run());
    co_await p.stop();
    if (fut.failed()) {
        std::rethrow_exception(fut.get_exception());
    }
    co_return p.take_results();
}

ss::future<> run(ss::sstring input_file) {
    std::vector<ss::future<results_map>> shard_results;
    size_t size = co_await ss::file_size(input_file);
    size_t per_shard = size / ss::smp::count;
    size_t offset = 0;
    for (auto shard : ss::smp::all_cpus()) {
        size_t len = per_shard;
        if (shard == (ss::smp::count - 1)) {
            // Last shard needs to take the result of the file.
            len = size - offset;
        }
        shard_results.push_back(
          ss::smp::submit_to(shard, [&input_file, offset, len]() {
              return run_one(
                input_file,
                {
                  .offset = offset,
                  .len = len,
                });
          }));
        offset += len;
    }
    results_map results;
    for (auto& fut : shard_results) {
        auto shard_result = co_await std::move(fut);
        for (const auto& [loc, agg] : shard_result) {
            results[loc].merge(agg);
        }
    }
    print_results(results);
}

int main(int argc, char** argv) {
    ss::app_template app;
    {
        namespace po = boost::program_options;
        app.add_options()(
          "input-file",
          po::value<seastar::sstring>()->default_value("measurements.txt"),
          "The input file containing all the weather station readouts.");
    }

    return app.run(argc, argv, [&app] {
        const auto& opts = app.configuration();
        return run(opts["input-file"].as<ss::sstring>());
    });
}
