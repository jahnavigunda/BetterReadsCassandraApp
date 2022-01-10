package com.betterreads.Goodreadsdataloader;

import com.betterreads.Goodreadsdataloader.author.Author;
import com.betterreads.Goodreadsdataloader.author.AuthorRepository;
import com.betterreads.Goodreadsdataloader.book.Book;
import com.betterreads.Goodreadsdataloader.book.BookRepository;
import connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class GoodReadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;
    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(GoodReadsDataLoaderApplication.class, args);
    }

    private void initAuthors() throws  IOException {
        Path path = Paths.get(authorDumpLocation);

        Stream<String> lines = Files.lines(path);

            lines.forEach(line -> {

                //read and parse the line .
                String json = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(json);

                //construct Author object.
                Author author = new Author();
                author.setName(jsonObject.optString("name"));
                author.setPersonalName(jsonObject.optString("personal_name"));
                author.setId(jsonObject.optString("key").replace("/authors/", ""));
                System.out.println("saving author: "+author.getName()+"............");
                // persist using repository
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });


    }

    private void initWorks() throws IOException {
        Path path = Paths.get(worksDumpLocation);
        Stream<String> lines = Files.lines(path);

        lines.forEach(line -> {

            //read and parse the line .
            String json = line.substring(line.indexOf("{"));
            try {
                JSONObject jsonObject = new JSONObject(json);

                //construct Book object.
                Book book = new Book();

                book.setId(jsonObject.getString("key").replace("/works/", ""));

                book.setName(jsonObject.optString("title"));

                JSONObject descriptionObj = jsonObject.optJSONObject("description");
                if(descriptionObj != null){
                    book.setDescription(descriptionObj.optString("value"));
                }

                JSONObject publishedDateObj = jsonObject.optJSONObject("created");
                if(publishedDateObj != null){
                    String dateStr = publishedDateObj.getString("value");
                    //parsing date to --  DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-DD'T'HH:mm:ss.SSSSSS");
                    book.setPublishedDate(LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(dateStr)));
                }

                JSONArray coverIdsArr = jsonObject.optJSONArray("covers");
                if(coverIdsArr  != null){
                    List<String> coverIds = new ArrayList<>();
                    for(int i=0; i<coverIdsArr.length(); i++){
                        coverIds.add(coverIdsArr.getString(i));
                    }
                    book.setCoverIds(coverIds);
                }

                JSONArray authorsArr = jsonObject.optJSONArray("authors");
                if(authorsArr != null){
                    List<String> authorIds = new ArrayList<>();
                    for(int i=0; i<authorsArr.length(); i++){
                        String authorId = authorsArr.getJSONObject(i).getJSONObject("author").getString("key")
                                .replace("/authors/", "");
                        authorIds.add(authorId);
                    }
                    book.setAuthorIds(authorIds);

                    //to get author names from author id.
                    List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                            .map(optionalAuthor -> {
                                if (!optionalAuthor.isPresent()) return "Unknown Author";
                                return optionalAuthor.get().getName();
                            }).collect(Collectors.toList());

                    book.setAuthorNames(authorNames);
                }

                // persist using repository
                System.out.println("Saving book details: "+book.getName()+".............");
                bookRepository.save(book);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        });
    }

    @PostConstruct
    public void start() throws IOException {

        //initAuthors();
        initWorks();
    }


    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
